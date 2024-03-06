/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.ship.upload;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.aliyun.odps.ship.common.CommandType;
import org.apache.commons.cli.ParseException;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.DshipStopWatch;
import com.aliyun.odps.ship.common.RecordConverter;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class BlockUploader {

  private BlockInfo blockInfo;
  private Long blockId;

  private TunnelUpdateSession updateSession;
  private SessionHistory sessionHistory;

  private boolean isScan;

  private boolean isDiscardBadRecord;
  private boolean isStrictSchema;
  private long badRecords;
  private long maxBadRecords = Constants.DEFAULT_BAD_RECORDS;

  private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private long startTime = 0;
  private long preTime = 0;
  private DshipStopWatch localIOStopWatch;
  private DshipStopWatch tunnelIOStopWatch;
  private boolean isCsv;
  private boolean printIOElapsedTime;

  protected boolean isUpsert;

  public BlockUploader(BlockInfo blockInfo, TunnelUpdateSession tus, SessionHistory sh)
      throws IOException {
    this(blockInfo, tus, sh, false);
  }

  public BlockUploader(BlockInfo blockInfo, TunnelUpdateSession tus, SessionHistory sh,
                       boolean isCsv) {
    this.isUpsert = tus.getCommandType().equals(CommandType.upsert);
    this.blockInfo = blockInfo;
    this.blockId = blockInfo.getBlockId();

    this.updateSession = tus;
    this.sessionHistory = sh;
    this.isScan = tus.isScan();

    isDiscardBadRecord = Boolean.valueOf(DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
    isStrictSchema = Boolean.valueOf(DshipContext.INSTANCE.get(Constants.STRICT_SCHEMA));
    printIOElapsedTime = Boolean.valueOf(DshipContext.INSTANCE.get(Constants.TIME));
    localIOStopWatch = new DshipStopWatch("local I/O", printIOElapsedTime);
    tunnelIOStopWatch = new DshipStopWatch("tunnel I/O", printIOElapsedTime);
    badRecords = 0;
    if (DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS) != null) {
      maxBadRecords = Long.valueOf(DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS));
    }
    this.isCsv = isCsv;
  }

  public void upload()
      throws TunnelException, IOException, ParseException {
    startTime = System.currentTimeMillis();
    preTime = System.currentTimeMillis();

    String type = isUpsert ? "upsert" : (isScan ? "scan" : "upload");
    print(type + " block: '" + blockId + "'\n");
    sessionHistory.log("start " + type + " , blockid=" + blockId);
    sessionHistory.saveContext();

    //if upsert block fail, retry 5 time.
    int retry = 1;
    while (true) {
      try {
        doUpdate();
        break;
      } catch (TunnelException e) {
        sessionHistory.log("retry:" + retry + "  " + Util.getStack(e));
        if (retry > Constants.RETRY_LIMIT) {
          DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.resume.toString());
          sessionHistory.saveContext();
          throw e;
        }
        print("update block " + blockId + " fail, retry:" + retry + "\n");
      } catch (IOException e) {
        sessionHistory.log("retry:" + retry + "  " + Util.getStack(e));
        if (retry > Constants.RETRY_LIMIT) {
          DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.resume.toString());
          sessionHistory.saveContext();
          throw e;
        }
        print("update block " + blockId + " fail, retry:" + retry + "\n");
      }
      retry++;
      try {
        Thread.sleep(Constants.RETRY_INTERNAL);
      } catch (InterruptedException e) {
        throw new UserInterruptException(e.getMessage());
      }
    }

    sessionHistory.log(type + " complete, blockid=" + blockId);
    StringBuilder messageBuilder = new StringBuilder();
    messageBuilder.append(String.format("%s block complete, block id: %d%s",
                                        type,
                                        blockId,
                                        (badRecords > 0 ? " [bad " + badRecords + "]" : "")));
    if (!isScan) {
      messageBuilder.append(localIOStopWatch.getFormattedSummary());
      messageBuilder.append(tunnelIOStopWatch.getFormattedSummary());
    }
    messageBuilder.append("\n");
    print(messageBuilder.toString());
  }


  private boolean doUpdate() throws TunnelException, IOException, ParseException {
    // clear bad data for new block upsert
    sessionHistory.clearBadData(blockId);

    //init reader/writer
    RecordReader reader = createReader();

    RecordConverter recordConverter = createRecordConverter(reader.getDetectedCharset());

    RecordWriter writer = updateSession.getWriter(blockId);
    updateSession.initRecord();

    while (true) {
      try {
        byte[][] textRecord = readAndTime(reader);

        if (textRecord == null) {
          break;
        }
        Record r = updateSession.getRecord(recordConverter, textRecord);
        writeAndTime(writer, r);
        printProgress(reader.getReadBytes(), false);
        ODPSConsoleUtils.checkThreadInterrupted();
      } catch (ParseException e) {
        String currentLine = reader.getCurrentLine();
        long offset = reader.getReadBytes() + blockInfo.getStartPos() - currentLine.length();
        if (currentLine.length() > 100) {
          currentLine = currentLine.substring(0, 100) + " ...";
        }
        String errMsg = e.getMessage() + "content: " + currentLine + "\noffset: " + offset + "\n";
        if (isDiscardBadRecord) {
          print(errMsg);
          checkDiscardBadData();
          // save bad data
          sessionHistory.saveBadData(reader.getCurrentLine() + DshipContext.INSTANCE.get(
              Constants.RECORD_DELIMITER), Long.valueOf(blockId));
        } else {
          DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.failed.toString());
          sessionHistory.saveContext();
          throw new ParseException(errMsg);
        }
      }
    }
    writer.close();
    reader.close();

    if (!isScan) {
      printProgress(reader.getReadBytes(), true);
      sessionHistory.saveFinishBlock(blockInfo);
    }
    return false;
  }

  private RecordReader createReader() throws IOException {
    RecordReader reader;
    boolean ignoreHeader = "true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.HEADER));

    if (isCsv) {
      reader =
          new CsvRecordReader(blockInfo, DshipContext.INSTANCE.get(Constants.CHARSET),
                              ignoreHeader);
    } else {
      String fieldDelimiter = DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER);
      String recordDelimiter = DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER);
      reader = new BlockRecordReader(blockInfo, fieldDelimiter, recordDelimiter, ignoreHeader);
    }
    return reader;
  }

  private RecordConverter createRecordConverter(String detectedCharset)
      throws UnsupportedEncodingException {

    String
        charset =
        detectedCharset == null ? DshipContext.INSTANCE.get(Constants.CHARSET) : detectedCharset;
    String ni = DshipContext.INSTANCE.get(Constants.NULL_INDICATOR);
    String dfp = DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN);
    String tz = DshipContext.INSTANCE.get(Constants.TIME_ZONE);
    RecordConverter
        recordConverter =
        new RecordConverter(updateSession.getSchema(), ni, dfp, tz, charset, false, isStrictSchema);
    return recordConverter;
  }

  private void printProgress(long cb, boolean summary) {
    if (updateSession.isScan()) {
      return;
    }
    long currTime = System.currentTimeMillis();
    //update progress every 5 seconds
    int updateGap = 5000;
    if(!summary && currTime - preTime <= updateGap) {
      return;
    }

    long gap = (currTime - startTime) / 1000;
    long length = blockInfo.getLength();
    if (gap > 0 && length > 0) {
      long cspeed = cb / gap;
      long percent = cb * 100 / length;
      StringBuilder messageBuilder = new StringBuilder();
      messageBuilder.append(String.format("Block info: %s, progress: %d%%, bs: %s, speed: %s/s",
                                          blockInfo.toString(),
                                          percent,
                                          Util.toReadableBytes(cb),
                                          Util.toReadableBytes(cspeed)));
      messageBuilder.append(localIOStopWatch.getFormattedSummary());
      messageBuilder.append(tunnelIOStopWatch.getFormattedSummary());
      messageBuilder.append("\n");
      print(messageBuilder.toString());
      preTime = currTime;
    }
  }

  private void print(String msg) {
    Instant instant = Instant.now();
    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
    String processStr = dateTimeFormatter.format(zonedDateTime) + "\t";
    System.err.print(processStr + msg);
  }

  private void checkDiscardBadData() throws ParseException, IOException {
    badRecords++;
    if (badRecords > maxBadRecords) {
      DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.failed.toString());
      sessionHistory.saveContext();
      throw new ParseException(Constants.ERROR_INDICATOR + "bad records exceed " + maxBadRecords);
    }
  }

  private void writeAndTime(RecordWriter writer, Record record) throws IOException {
    tunnelIOStopWatch.resume();
    try {
      writer.write(record);
    } finally {
      tunnelIOStopWatch.suspend();
    }
  }

  private byte[][] readAndTime(RecordReader reader) throws IOException {
    localIOStopWatch.resume();
    try {
      return reader.readTextRecord();
    } finally {
      localIOStopWatch.suspend();
    }
  }
}

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
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.ParseException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.RecordConverter;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.tunnel.TunnelException;

import jline.console.UserInterruptException;

/**
 * Created by lulu on 15-1-29.
 */
public class BlockUploader {
  private BlockInfo blockInfo;
  private Long blockId;

  private TunnelUploadSession uploadSession;
  private SessionHistory sessionHistory;

  private boolean isScan;

  private boolean isDiscardBadRecord;
  // bad records
  private long badRecords;
  private long maxBadRecords = Constants.DEFAULT_BAD_RECORDS;

  private DecimalFormat decimalFormat = new DecimalFormat("###,###");
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private long startTime = 0;
  private long preTime = 0;

  public BlockUploader(BlockInfo blockInfo, TunnelUploadSession tus, SessionHistory sh)
    throws IOException {
      this.blockInfo = blockInfo;
      this.blockId = blockInfo.getBlockId();

      this.uploadSession = tus;
      this.sessionHistory = sh;
      this.isScan = tus.isScan();

      isDiscardBadRecord = Boolean.valueOf(DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS));
      badRecords = 0;
      if (DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS) != null) {
        maxBadRecords = Long.valueOf(DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS));
      }
    }

  public void upload() 
      throws TunnelException, IOException, ParseException {
    startTime = System.currentTimeMillis();
    preTime = System.currentTimeMillis();
    String type = isScan ? "scan" : "upload";
    print(type + " block: '" + blockId + "'\n");
    sessionHistory.log("start " + type + " , blockid=" + blockId);

    sessionHistory.saveContext();

    //if upload block fail, retry 5 time.
    int retry = 1;
    while (true) {
      try {
        doUpload();
        break;
      } catch (TunnelException e) {
        sessionHistory.log("retry:" + retry + "  " + Util.getStack(e));
        if (retry > Constants.RETRY_LIMIT) {
          DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.resume.toString());
          sessionHistory.saveContext();
          throw e;
        }
        print("upload block "+ blockId + " fail, retry:" + retry+"\n");
      } catch (IOException e) {
        sessionHistory.log("retry:" + retry + "  " + Util.getStack(e));
        if (retry > Constants.RETRY_LIMIT) {
          DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.resume.toString());
          sessionHistory.saveContext();
          throw e;
        }
        print("upload block "+ blockId + " fail, retry:" + retry+"\n");
      }
      retry++;
      try {
        Thread.sleep(Constants.RETRY_INTERNAL);
      } catch (InterruptedException e) {
        throw new UserInterruptException(e.getMessage());
      }
    }

    sessionHistory.log(type + " complete, blockid=" + blockId);
    print(type + " block complete, blockid=" + blockId+ (badRecords>0 ? " [bad " + badRecords + "]":"") + "\n");
  }


  private boolean doUpload() throws TunnelException, IOException, ParseException {
    // clear bad data for new block upload
    sessionHistory.clearBadData(Long.valueOf(blockId));

    //init reader/writer
    BlockRecordReader reader = createReader();

    RecordConverter recordConverter = createRecordConverter(reader.getDetectedCharset());

    RecordWriter writer = uploadSession.getWriter(Long.valueOf(blockId));

    while (true) {
      try {
        byte[][] textRecord = reader.readTextRecord();
        if (textRecord == null) break;

        Record r = recordConverter.parse(textRecord);
        writer.write(r);
        printProgress(reader.getReadBytes(), false);
      } catch (ParseException e) {
        String currentLine = reader.getCurrentLine();
        long offset = reader.getReadBytes() + blockInfo.getStartPos() - currentLine.length();
        if (currentLine.length() > 100) {
          currentLine = currentLine.substring(0, 100) + " ...";
        }
        String errMsg = e.getMessage() + "content: " +  currentLine + "\noffset: " + offset + "\n";
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

  private BlockRecordReader createReader() throws IOException {
    boolean ignoreHeader = "true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.HEADER));
    String fieldDelimiter = DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER);
    String recordDelimiter = DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER);
    BlockRecordReader reader = new BlockRecordReader(blockInfo, fieldDelimiter, recordDelimiter, ignoreHeader);
    return reader;
  }

  private RecordConverter createRecordConverter(String detectedCharset) throws UnsupportedEncodingException {

    String charset = detectedCharset == null ? DshipContext.INSTANCE.get(Constants.CHARSET) : detectedCharset;
    String ni = DshipContext.INSTANCE.get(Constants.NULL_INDICATOR);
    String dfp = DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN);
    String tz = DshipContext.INSTANCE.get(Constants.TIME_ZONE);
    RecordConverter recordConverter = new RecordConverter(uploadSession.getSchema(), ni, dfp, tz, charset);

    return recordConverter;
  }

  private void printProgress(long cb, boolean summary) {

    if (uploadSession.isScan()) {
      return;
    }

    long currTime = System.currentTimeMillis();
    long gap = (currTime - startTime) / 1000;
    long length = blockInfo.getLength();
    //update progress every 5 seconds
    if ((currTime - preTime > 5000 || summary) && gap > 0 && length > 0) {
      long cspeed = cb / gap / 1024;
      long percent = cb * 100 / length;
      print(blockInfo.toString() + "\t" + percent + "%\t" + decimalFormat.format(cb / 1024) + " KB\t" +
          decimalFormat.format(cspeed) + " KB/s\n");
      preTime = currTime;
    }
  }

  private void print(String msg) {
    Date date = new Date();
    String processStr = dateFormat.format(date) + "\t";
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
}

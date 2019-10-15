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

package com.aliyun.odps.ship.download;

import com.aliyun.odps.ship.common.DshipStopWatch;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.common.RecordConverter;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class FileDownloader {

  private final String path;
  private final Long start;
  private final Long end;
  private final Long id;  // 全局 id
  SessionHistory sh;

  TunnelDownloadSession ds;
  File file;
  private long writtenBytes = 0;

  private long currTime;
  private long preTime;
  private DshipStopWatch localIOStopWatch;
  private DshipStopWatch tunnelIOStopWatch;
  private RecordWriter writer;
  SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private boolean isCsv = false;
  private boolean printIOElapsedTime = false;

  public FileDownloader(String path, Long id, Long start, Long end, TunnelDownloadSession ds, SessionHistory sh) throws FileNotFoundException, IOException {
    this(path, id, start, end, ds, sh, false);
  }


  public FileDownloader(String path, Long id, Long start, Long end, TunnelDownloadSession ds, SessionHistory sh, boolean isCsv) throws FileNotFoundException, IOException {
    OptionsBuilder.checkParameters("download");
    this.path = path;
    this.file = new File(path);
    if (!file.getAbsoluteFile().getParentFile().exists()) {
      file.getAbsoluteFile().getParentFile().mkdirs();
    }
    this.id = id;
    this.start = start;
    this.end = end;
    this.ds = ds;
    this.sh = sh;
    this.isCsv = isCsv;
    this.printIOElapsedTime = Boolean.valueOf(DshipContext.INSTANCE.get(Constants.TIME));
    localIOStopWatch = new DshipStopWatch("local I/O", printIOElapsedTime);
    tunnelIOStopWatch = new DshipStopWatch("tunnel I/O", printIOElapsedTime);

    if (sh != null) {
      String msg = String.format("file [%d]: [%s, %s), %s", id, Util.toReadableNumber(start),
                                 Util.toReadableNumber(end), path);
      sh.log(msg);
      System.err.println(sim.format(new Date()) + "  -  " + msg);
    }
  }

  public void download() throws IOException, TunnelException {
    if (sh != null) {
      String msg = String.format("file [" + id + "] start");
      sh.log(msg);
      System.err.println(sim.format(new Date()) + "  -  " + msg);
    }

    String fd = DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER);
    String rd = DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER);
    String ni = DshipContext.INSTANCE.get(Constants.NULL_INDICATOR);
    String dfp = DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN);
    String tz = DshipContext.INSTANCE.get(Constants.TIME_ZONE);
    String charset = DshipContext.INSTANCE.get(Constants.CHARSET);

    boolean exponential = false;
    String e = DshipContext.INSTANCE.get(Constants.EXPONENTIAL);
    if (e != null && e.equalsIgnoreCase("true")) {
      exponential = true;
    }

    if (isCsv) {
      writer = new CsvRecordWriter(file, charset);
    } else {
      writer = new TextRecordWriter(file, fd, rd);
    }

    TableSchema schema = ds.getSchema();

    RecordConverter converter = new RecordConverter(schema, ni, dfp, tz, charset, exponential);

    if ("true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.HEADER))) {
      writeHeader(writer, schema);
    }

    preTime = System.currentTimeMillis();
    DshipRecordReader recordReader = ds.getRecordReader(start, end);
    long count = 0;
    Record r;

    while ((r = readAndTime(recordReader)) != null) {
      writeAndTime(writer, converter.format(r));
      count++;
      currTime = System.currentTimeMillis();
      // 5秒一次输出
      if (currTime - preTime > 5000) {
        printProgress(count);
        preTime = currTime;
      }
      ODPSConsoleUtils.checkThreadInterrupted();
    }
    writer.close();
    writtenBytes = writer.getWrittedBytes();
    if (sh != null) {
      StringBuilder messageBuilder = new StringBuilder();
      messageBuilder.append(String.format("file [%d] OK. total: %s",
          id,
          Util.toReadableBytes(writtenBytes)));
      messageBuilder.append(localIOStopWatch.getFormattedSummary());
      messageBuilder.append(tunnelIOStopWatch.getFormattedSummary());
      System.err.println(sim.format(new Date()) + "  -  " + messageBuilder.toString());
      sh.log(messageBuilder.toString());
    }
  }

  private void printProgress(long count) throws IOException {
    if (end - start == 0) {
      return;
    }

    long speed = 0; // bytes per sec
    if (writer.getWrittedBytes() - writtenBytes > 0 && currTime - preTime > 0) {
      speed = (writer.getWrittedBytes() - writtenBytes) / (currTime - preTime) * 1000;
    }
    writtenBytes = writer.getWrittedBytes();
    long percentage = (count * 100 / (end - start));

    int threads = Integer.parseInt(DshipContext.INSTANCE.get(Constants.THREADS));
    StringBuilder msgBuilder = new StringBuilder();
    if (threads > 1) {
      long threadId = Thread.currentThread().getId() % threads;
      msgBuilder.append(String.format("Thread %d: ", threadId));
    }
    msgBuilder.append(String.format("[%d] %d%%, %s records downloaded, %s/s, ",
        id,
        percentage,
        Util.toReadableNumber(count),
        Util.toReadableBytes(speed)));
    msgBuilder.append(localIOStopWatch.getFormattedSummary());
    msgBuilder.append(tunnelIOStopWatch.getFormattedSummary());
    System.err.println(sim.format(new Date()) + "  -  " + msgBuilder.toString());
  }

  private void writeHeader(RecordWriter writer, TableSchema schema) throws IOException {
    byte[][] headers = new byte[schema.getColumns().size()][];
    for (int i = 0; i < schema.getColumns().size(); i++) {
      String charset = DshipContext.INSTANCE.get(Constants.CHARSET);
      // schema column 没有直接 getBytes 的接口，实际上无法支持 ignore charset。不过这种场景应该也很罕见
      headers[i] = schema.getColumn(i).getName().getBytes(
          Util.isIgnoreCharset(charset) ? Constants.REMOTE_CHARSET : charset);
    }
    writeAndTime(writer, headers);
  }

  private Record readAndTime(DshipRecordReader recordReader)
      throws TunnelException, IOException{
    tunnelIOStopWatch.resume();
    try {
        return recordReader.next();
    } finally {
        tunnelIOStopWatch.suspend();
    }
  }

  private void writeAndTime(RecordWriter writer, byte[][] record) throws IOException {
    localIOStopWatch.resume();
    try {
        writer.write(record);
    } finally {
        localIOStopWatch.suspend();
    }
  }

  public long getWrittenBytes() {
    return writtenBytes;
  }
}

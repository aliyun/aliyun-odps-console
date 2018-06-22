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

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

/**
 * Created by nizheming on 15/5/27.
 */
public class DshipRecordReader {

  private TunnelDownloadSession download;
  private Long end;
  TunnelRecordReader tunnelReader;
  private long currentLines;
  int retry = 0;
  private Record r = null;
  private List<Column> columns = null;

  public DshipRecordReader(TunnelDownloadSession download, Long start, Long end, List<Column> columns) throws IOException, TunnelException {
    this.download = download;
    this.end = end;
    this.currentLines = start;
    this.columns = columns;
    if (!start.equals(end)) {
      initReader(null);
    }
  }

  public Record next() throws TunnelException, IOException {
    if (tunnelReader == null) {
      return null;
    }
    try {
      r = tunnelReader.read(r);
      currentLines++;
      retry = 0;
      return r;
    } catch (IOException e) {
      initReader(e);
      return next();
    }
  }

  protected void initReader(Exception throwedException) throws TunnelException, IOException {

    retry++;
    if (retry > 5) {
      if (throwedException != null) {
        throw new IOException(Constants.ERROR_INDICATOR + "download read error, retry exceed 5.\n" + throwedException.getMessage(),
                              throwedException);
      } else {
        throw new IOException(Constants.ERROR_INDICATOR + "download read error, retry exceed 5.");
      }
    }
    if (tunnelReader != null) {
      tunnelReader.close();
      tunnelReader = null;
    }

    try {
      tunnelReader = (TunnelRecordReader) openRecordReader(currentLines);
    } catch (Exception e) {
      SessionHistoryManager.createSessionHistory(download.getDownloadId()).log("retry:" + retry + "  " + Util.getStack(e));
      initReader(e);
    }
  }

  protected RecordReader openRecordReader(long currentLines) throws TunnelException, IOException {
    if (currentLines > end) {
      throw new TunnelException(Constants.ERROR_INDICATOR + "current lines: " + currentLines + " end: " + end);
    }
    return download.openRecordReader(currentLines, end - currentLines,
                                     Boolean.valueOf(DshipContext.INSTANCE.get(Constants.COMPRESS)), columns);
  }
}

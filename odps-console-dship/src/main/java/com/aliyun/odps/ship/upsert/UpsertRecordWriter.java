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

package com.aliyun.odps.ship.upsert;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;

public class UpsertRecordWriter implements RecordWriter {
  private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private UpsertStream.Listener listener;
  private SessionHistory sessionHistory;
  private UpsertStream stream;

  public UpsertRecordWriter(TableTunnel.UpsertSession upsertSession) throws IOException, TunnelException {
    String sessionId = upsertSession.getId();
    sessionHistory = SessionHistoryManager.createSessionHistory(sessionId);
    listener = new UpsertStream.Listener() {

      @Override
      public void onFlush(UpsertStream.FlushResult result) {
        // pass
      }

      @Override
      public boolean onFlushFail(String error, int retry) {
        try {
          sessionHistory.log(String.format("flush failed %s, retry times: %d", error, retry));
        } catch (IOException e) {
          e.printStackTrace();
        }
        print(String.format("flush failed %s, retry times: %d", error, retry));
        return false;
      }
    };
    stream = upsertSession.buildUpsertStream().setListener(listener).build();
  }

  @Override
  public void write(Record record) throws IOException {
    try {
      stream.upsert(record);
    } catch (TunnelException e) {
      sessionHistory.log("UpsertStream upsert failed: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      stream.flush();
      stream.close();
    } catch (TunnelException e) {
      sessionHistory.log("UpsertStream commit failed: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void print(String msg) {
    Instant instant = Instant.now();
    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
    String processStr = dateTimeFormatter.format(zonedDateTime) + "\t";
    System.err.println(processStr + msg);
  }
}

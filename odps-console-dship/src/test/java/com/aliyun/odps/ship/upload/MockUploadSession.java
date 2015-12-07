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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.TunnelException;

public class MockUploadSession extends TunnelUploadSession {

  public String sid;

  public MockUploadSession() throws TunnelException, IOException {
    super("just for test");

    if (DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID) != null) {
      sid = DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID);
    }
  }

  @Override
  public TableSchema getSchema() {

    // 123||ab测试c||20130308101010||true||21.2||123456.1234
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    rs.addColumn(new Column("doub1", OdpsType.DOUBLE));

    return rs;
  }

  @Override
  public String getUploadId() {
    return sid == null ? "mock-upload-id" : sid;
  }

  @Override
  public boolean isScan() {
    return false;
  }

  @Override
  public RecordWriter getWriter(long bId) throws TunnelException, IOException {
    return new ScanerWriter();
  }

  //@Override
  public void complete() throws TunnelException, IOException {

    SessionHistory sh = SessionHistoryManager.createSessionHistory(getUploadId());
    sh.loadContext();
    assertEquals("not running", SessionStatus.running.toString(),
                 DshipContext.INSTANCE.get(Constants.STATUS));

    // clear log
    String log = Util.getSessionDir(sid) + "/log.txt";
    File f = new File(log);
    if (f.exists()) {
      f.delete();
    }
    
    if ("mock-upload-id".equals(sid)){
      File sf = new File(Util.getSessionDir(sid));
      File[] sfl = sf.listFiles();
      for (File s : sfl){
        s.delete();
      }
      sf.delete();
    }
  }
  
  public void abort() throws TunnelException, IOException {
  }

  public void clearSession() throws Exception {
    SessionHistory sh = SessionHistoryManager.createSessionHistory(getUploadId());
    sh.delete();
  }

}

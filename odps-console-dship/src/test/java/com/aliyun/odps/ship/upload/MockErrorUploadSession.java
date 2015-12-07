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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * 模拟上传出错，fileuploader进行重试
 * */
public class MockErrorUploadSession extends TunnelUploadSession {

  int error = 0;
  long blockId;
  int crash = 5;

  public MockErrorUploadSession() throws TunnelException, IOException {
    super("just for test");
  }

  public void setCrash(int crash) {
    this.crash = crash;
  }

  @Override
  public boolean isScan() {
    return false;
  }

  //@Override
  public void complete() throws TunnelException, IOException {
    // clear log
    String log = Util.getSessionDir(getUploadId()) + "/log.txt";
    File f = new File(log);
    if (f.exists()) {
      f.delete();
    }

  }

  @Override
  public String getUploadId() {
    return "mock-upload-id";
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
  public RecordWriter getWriter(long bId) throws TunnelException, IOException {

    blockId = bId;
    return new MockWriter();
  }

  class MockWriter implements RecordWriter {

    @Override
    public void close() throws IOException {
      // do nothing
    }

//    @Override
//    public long getReadedBytes() {
//      // do nothing
//      return 0;
//    }

    @Override
    public void write(Record r) throws IOException {

        if (error < crash) {
          error++;
          throw new IOException("write error");
        }
    }
  }

}

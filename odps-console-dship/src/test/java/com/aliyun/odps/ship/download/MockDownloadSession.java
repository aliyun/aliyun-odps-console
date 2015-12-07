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
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.DefaultConnection;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

public class MockDownloadSession extends TunnelDownloadSession {

  static int crash = 0;
  static int alreadCrash = 0;

  public MockDownloadSession() throws TunnelException, IOException {
    super(10);
  }

//  @Override
//  public void complete() throws TunnelException, IOException {
//    
//    // clear log
//    String log = Util.getRootDir() + "/sessions/" + getDownloadId() + "/log.txt";
//    File f = new File(log);
//    if (f.exists()) {
//      f.delete();
//    }
//
//  }

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
  public String getDownloadId() {
    return "mock-download-id";
  }

  @Override
  public DshipRecordReader getRecordReader(Long start, Long end) throws IOException, TunnelException {
    return new DshipRecordReader(null, start, end) {
      @Override
      protected RecordReader openRecordReader(long currentLines) throws TunnelException, IOException {
        return new MockReader(currentLines);
      }
    };
  }
}

class MockReader extends TunnelRecordReader implements RecordReader  {


  private static final TableSchema rs;
  private static final DefaultConnection connection;

  static {
    rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    rs.addColumn(new Column("doub1", OdpsType.DOUBLE));
    connection = new DefaultConnection() {
      @Override
      public InputStream getInputStream() throws IOException {
        return null;
      }
    };
  }
  long lines;
  public MockReader(long lines) throws IOException {

    super(rs, connection, new CompressOption());
    this.lines = lines;

  }

  @Override
  public void close() throws IOException {}

  @Override
  public Record read() throws IOException{

    lines++;
    if (lines > 10) {
      return null;
    }

    if (MockDownloadSession.alreadCrash >= MockDownloadSession.crash) {

      MockDownloadSession.alreadCrash = 0;
      
      TableSchema rs = new TableSchema();
      rs.addColumn(new Column("i1", OdpsType.BIGINT));
      rs.addColumn(new Column("s1", OdpsType.STRING));
      rs.addColumn(new Column("d1", OdpsType.DATETIME));
      rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
      rs.addColumn(new Column("de1", OdpsType.DOUBLE));
      rs.addColumn(new Column("doub1", OdpsType.DOUBLE));

//      downloa
      Record r = new ArrayRecord(rs.getColumns().toArray(new Column[0]));
      
//      Record r = new Record(6);
      SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
      sf.setTimeZone(TimeZone.getTimeZone("GMT"));
      Date d = null;
      try {
        d = sf.parse("20130508101010");
      } catch (ParseException e) {
        e.printStackTrace();
      }
      r.setBigint(0, lines);
      r.setString(1, "bb你好b");
      r.setDatetime(2, d);
      r.setBoolean(3, true);
      r.setDouble(4, Double.valueOf("2.2"));
      r.setDouble(5, Double.valueOf("1234.1234"));
      return r;
    } else {
      MockDownloadSession.alreadCrash++;
      throw new IOException("crash :" + MockDownloadSession.alreadCrash);
    }
  }

  @Override
  public Record read(Record r) throws IOException {
    return read();
  }
//  @Override
//  public long getReadedBytes() {
//    return 0;
//  }
}

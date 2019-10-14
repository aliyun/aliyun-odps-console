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
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
  TableSchema rs;

  public MockDownloadSession() throws TunnelException, IOException {
    super(10);

    rs = new TableSchema();
    // 123||ab测试c||20130308101010||true||21.2||123456.1234

    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    rs.addColumn(new Column("doub1", OdpsType.DECIMAL));
  }

  @Override
  public TableSchema getSchema() {
    return rs;
  }

  public void setSchema(TableSchema schema) {
    this.rs = schema;
  }

  @Override
  public String getDownloadId() {
    return "mock-download-id";
  }

  @Override
  public DshipRecordReader getRecordReader(Long start, Long end)
      throws IOException, TunnelException {
    return new DshipRecordReader(null, start, end, null) {
      @Override
      protected RecordReader openRecordReader(long currentLines)
          throws TunnelException, IOException {
        return new MockReader(currentLines, getSchema().getColumns());
      }
    };
  }
}

class MockReader extends TunnelRecordReader implements RecordReader {


  private static final TableSchema rs;
  private static final DefaultConnection connection;
  private List<Column> columns = null;

  static {
    rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    rs.addColumn(new Column("doub1", OdpsType.DECIMAL));
    connection = new DefaultConnection() {
      @Override
      public InputStream getInputStream() throws IOException {
        return null;
      }
    };
  }

  long lines;

  public MockReader(long lines, List<Column> columns) throws IOException {

    super(rs, connection, new CompressOption());
    this.lines = lines;
    this.columns = columns;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Record read() throws IOException {

    lines++;
    if (lines > 10) {
      return null;
    }

    if (MockDownloadSession.alreadCrash >= MockDownloadSession.crash) {

      MockDownloadSession.alreadCrash = 0;

      if (columns == null) {
        columns = rs.getColumns();
      }
      //      downloa

      Record r = new ArrayRecord(columns.toArray(new Column[0]));

//      Record r = new Record(6);
      SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
//      sf.setTimeZone(TimeZone.getTimeZone("GMT"));
      Date d = null;
      try {
        d = sf.parse("20130508181010");
      } catch (ParseException e) {
        e.printStackTrace();
      }

      TableSchema schema = new TableSchema();
      schema.setColumns(columns);

      int i = 0;
      while (i < schema.getColumns().size()) {
        OdpsType type = schema.getColumn(i).getType();
        switch (type) {
          case BIGINT:
            r.setBigint(i, lines);
            break;
          case STRING:
            r.setString(i, "bb你好b");
            break;
          case DATETIME:
            r.setDatetime(i, d);
            break;
          case BOOLEAN:
            r.setBoolean(i, true);
            break;
          case DOUBLE:
            r.setDouble(i, Double.valueOf("2.2"));
            break;
          case DECIMAL:
            r.setDecimal(i, new BigDecimal("1234.1234"));
            break;
        }
        i++;
      }

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

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

package com.aliyun.openservices.odps.console;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.csvreader.CsvWriter;

public class CSVRecordPrinter extends RecordPrinter {

  private CsvWriter csv;
  private TableSchema schema;

  protected CSVRecordPrinter(TableSchema schema, ExecutionContext context) {
    super(schema, context);
    this.schema = schema;
    csv = new CsvWriter(context.getOutputWriter().getResultStream(), ',', StandardCharsets.UTF_8);
    csv.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
  }

  @Override
  public void printFrame() {
    // do nothing in csv mode
  }

  @Override
  public void printTitle() throws ODPSConsoleException {
    int n = schema.getColumns().size();
    String[] record = new String[n];
    for (int i = 0; i < n; i++) {
      record[i] = schema.getColumn(i).getName();
    }
    try {
      csv.writeRecord(record, true);
      csv.flush();
    } catch (IOException e) {
      throw new ODPSConsoleException(e);
    }
  }

  @Override
  public void printRecord(Record record) throws ODPSConsoleException {
    String[] r = new String[record.getColumnCount()];
    for (int i = 0; i < record.getColumnCount(); i++) {
      r[i] = formatField(record.get(i), record.getColumns()[i].getTypeInfo());
    }
    try {
      csv.writeRecord(r, true);
      csv.flush();
    } catch (IOException e) {
      throw new ODPSConsoleException(e);
    }
  }

}

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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class CSVRecordPrinterTest {

  @Test
  public void testDefaultDelimiter() throws Exception {
    String output = printRecord(',');

    Assert.assertTrue(output.contains("first,second"));
    Assert.assertTrue(output.contains("one,two"));
  }

  @Test
  public void testCustomDelimiter() throws Exception {
    String output = printRecord('|');

    Assert.assertTrue(output.contains("first|second"));
    Assert.assertTrue(output.contains("one|two"));
    Assert.assertFalse(output.contains("first,second"));
  }

  private String printRecord(char delimiter) throws Exception {
    ExecutionContext context = new ExecutionContext();
    context.setMachineReadable(true);
    context.setMachineReadableDelimiter(delimiter);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    context.setOutputWriter(new TestOutputWriter(context, output));

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("first", TypeInfoFactory.STRING));
    schema.addColumn(new Column("second", TypeInfoFactory.STRING));

    Record record = new ArrayRecord(schema);
    record.setString(0, "one");
    record.setString(1, "two");

    RecordPrinter printer = RecordPrinter.createReporter(schema, context);
    printer.printTitle();
    printer.printRecord(record);

    return output.toString(StandardCharsets.UTF_8.name());
  }

  private static class TestOutputWriter extends DefaultOutputWriter {

    private final OutputStream output;

    TestOutputWriter(ExecutionContext context, OutputStream output) {
      super(context);
      this.output = output;
    }

    @Override
    public OutputStream getResultStream() {
      return output;
    }
  }
}

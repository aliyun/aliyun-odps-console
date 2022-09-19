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

import java.util.Map;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.openservices.odps.console.utils.FormatUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class HumanReadableRecordPrinter extends RecordPrinter {

  private String frame;
  private String title;
  private Map<String, Integer> width;

  protected HumanReadableRecordPrinter(TableSchema schema, ExecutionContext context) {
    super(schema, context);

    width = ODPSConsoleUtils.getDisplayWidth(schema.getColumns(), null, null);
    frame = ODPSConsoleUtils.makeOutputFrame(width);
    title = ODPSConsoleUtils.makeTitle(schema.getColumns(), width);
  }

  private String formatRecord(Record record, Map<String, Integer> width) {
    StringBuilder sb = new StringBuilder();
    sb.append("| ");

    for (int i = 0; i < record.getColumnCount(); i++) {
      String res = null;
      if (context.isInteractiveQuery() && !context.isUseInstanceTunnel()
          && (record.get(i) == null || record.get(i).equals(com.aliyun.odps.Record.NULLINDICATOR))) {
        // session mode, we should convert "\N" to 'NULL'
        res = "NULL";
      } else {
        res = FormatUtils.formatField(record.get(i), record.getColumns()[i].getTypeInfo());
      }
      sb.append(res);
      if (res.length() < width.get(record.getColumns()[i].getName())) {
        int extraLen = width.get(record.getColumns()[i].getName()) - res.length();
        while (extraLen-- > 0) {
          sb.append(" ");
        }
      }

      sb.append(" | ");
    }

    return sb.toString();
  }

  @Override
  public void printFrame() {
    context.getOutputWriter().writeResult(frame);
  }

  @Override
  public void printTitle() {
    context.getOutputWriter().writeResult(title);
  }

  @Override
  public void printRecord(Record record) {
    context.getOutputWriter().writeResult(formatRecord(record, width));
  }
}

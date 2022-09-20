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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;

/**
 * Created by dongxiao on 2019/8/19.
 */
public abstract class RecordPrinter {
  TableSchema schema;
  ExecutionContext context;

  RecordPrinter(TableSchema schema, ExecutionContext context) {
    this.schema = schema;
    this.context = context;
  }

  public static RecordPrinter createReporter(TableSchema schema, ExecutionContext context) {
    if (context.isMachineReadable()) {
      return new CSVRecordPrinter(schema, context);
    } else {
      return new HumanReadableRecordPrinter(schema, context);
    }
  }

  public abstract void printFrame();

  public abstract void printTitle() throws ODPSConsoleException;

  public abstract void printRecord(Record record) throws ODPSConsoleException;

}

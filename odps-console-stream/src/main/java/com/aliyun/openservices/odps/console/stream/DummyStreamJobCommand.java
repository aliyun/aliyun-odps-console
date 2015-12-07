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

package com.aliyun.openservices.odps.console.stream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.stream.CreateStreamJobCommand;
import java.util.regex.*;

public class DummyStreamJobCommand extends CreateStreamJobCommand {
  private String dummySql;

  public DummyStreamJobCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    dummySql = commandText;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    String tmpSql = dummySql.toUpperCase().trim().replaceAll("\\s+", " ");
    if (tmpSql.endsWith("END STREAMJOB"))
    {
      throw new OdpsException("In front of end streamjob must have a semicolon.");
    }
    return;
  }

  public static DummyStreamJobCommand parse(String commandString, ExecutionContext sessionContext) {
    return null;
  }

}

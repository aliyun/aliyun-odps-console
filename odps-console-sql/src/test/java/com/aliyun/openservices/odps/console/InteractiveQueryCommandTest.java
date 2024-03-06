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

import com.aliyun.odps.OdpsException;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class InteractiveQueryCommandTest {

  private String[] sessionCMD = {
          "detach session",
          "attach session test",
          "attach session public.test",
  };

  @Test
  public void testParse() throws ODPSConsoleException, OdpsException {

    for (String cmd : sessionCMD) {
      System.out.println(cmd);
      ExecutionContext context = ExecutionContext.init();
      InteractiveQueryCommand command = InteractiveQueryCommand.parse(cmd, context);
      assertNotNull(command);
    }
  }
}

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

package com.aliyun.openservices.odps.console.pub;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by zhenhong.gzh on 2015/4/23.
 */
public class StopInstanceCommandTest {

  private String[] positive = {"kill -sync ", "kill ", "KiLl ", "KILL -sync "};

  private String[] badPara = {"kill -async ", "kill -a -sync ", "kill -sync "};

  private String[] nagtive = {"stop ID "};

  public String getID(ExecutionContext ctx) throws ODPSConsoleException, OdpsException {
    Odps odps = OdpsConnectionFactory.createOdps(ctx);
    String project = ctx.getProjectName();
    String sql = "select count(*) from " + project + ";";
    Instance i = SQLTask.run(odps, sql);

    return i.getId();
  }

  @Test
  public void testSyncPositive() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String str : positive) {
      String cmd = str + getID(ctx);
      StopInstanceCommand test = StopInstanceCommand.parse(cmd, ctx);

      assertNotNull(test);
      test.run();
    }
  }

  @Test
  public void testSyncNagtive() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String str : nagtive) {
      StopInstanceCommand test = StopInstanceCommand.parse(str, ctx);
      assertNull(test);
    }
  }

  @Test(expected = ODPSConsoleException.class)
  public void testSyncBadParas() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String str : badPara) {
      String cmd = str + getID(ctx);
      StopInstanceCommand.parse(cmd, ctx);
    }
  }
}

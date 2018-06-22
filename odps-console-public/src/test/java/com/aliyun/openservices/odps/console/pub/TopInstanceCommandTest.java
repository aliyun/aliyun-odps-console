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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by zhenhong.gzh on 2015/4/23.
 */
public class TopInstanceCommandTest {

  private String[]
      positive =
      {"top instance ", "toP instance ", "TOP INSTANCE ", "top instance -d", "top instance -d 10",
       "top instance -limit 2", "top instance -all", "top instance -p tt",
       "top instance -status running",
       "top instance -d -all -status running -limit 4 -p test_projecyt"};

  private String[] negative = {"top instances ", "tops instance"};

  private String[]
      error =
      {"top instance sldkfj", "top instance -status slkf", "top instance -d -all sdlfkj",
       "top instance -limit skdfj", "top instance -d -all -status xxx -p xx yyy",
       "top instance -status", "top instance -limit", "top instance -p", "top instance -d -10"};

  @Test

  public void testSyncPositive() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String str : positive) {
      TopInstanceCommand test = TopInstanceCommand.parse(str, ctx);

      assertNotNull(test);
    }
  }

  @Test
  public void testSyncNegative() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String str : negative) {
      TopInstanceCommand test = TopInstanceCommand.parse(str, ctx);
      assertNull(test);
    }
  }

  @Test
  public void testError() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    int count = 0;
    for (String str : error) {
      try {
        TopInstanceCommand test = TopInstanceCommand.parse(str, ctx);
      } catch (ODPSConsoleException e) {
        count++;
      }
    }

    assertEquals(error.length, count);
  }

  @Test
  public void testRun() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    TopInstanceCommand test = TopInstanceCommand.parse(positive[0], ctx);
    test.run();
  }

}

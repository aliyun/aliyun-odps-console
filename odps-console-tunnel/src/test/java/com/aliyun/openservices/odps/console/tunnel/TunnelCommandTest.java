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

package com.aliyun.openservices.odps.console.tunnel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.openservices.odps.console.ExecutionContext;
import java.io.File;
import org.junit.Test;

public class TunnelCommandTest {

  @Test
  public void test() throws Exception {
    ExecutionContext ctx = ExecutionContext.init();
    File temp = File.createTempFile("test", null);
    temp.deleteOnExit();
    DShipCommand cmd = TunnelCommand
        .parse(
            "upload pt_test2 partition(ds='q',pt='q') from " + temp.getAbsolutePath(),
            ctx);
    assertNotNull(cmd);

    String expectedCommandText = "tunnel upload -c \'utf8\' -dfp \'yyyy-MM-dd HH:mm:ss\' -fd \',\'"
        + " -rd \'\n\' -ni \'NULL\' -dbr 'false' " + temp.getAbsolutePath()
        + " pt_test2/ds='q',pt='q'";
    String commandText = cmd.getCommandText();
    assertEquals(expectedCommandText, commandText);

    cmd = TunnelCommand
        .parse(
            "download pt_test2 partition(ds='q',pt='q') to " + temp.getAbsolutePath(),
            ctx);
    assertNotNull(cmd);

    expectedCommandText = "tunnel download -c \'utf8\' -dfp \'yyyy-MM-dd HH:mm:ss\' -fd \',\'"
        + " -rd \'\n\' -ni \'NULL\' " + "pt_test2/ds='q',pt='q' " + temp.getAbsolutePath();
    commandText = cmd.getCommandText();
    assertEquals(expectedCommandText, commandText);
  }

}

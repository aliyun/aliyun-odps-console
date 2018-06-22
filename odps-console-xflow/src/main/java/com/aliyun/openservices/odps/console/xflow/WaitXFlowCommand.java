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

package com.aliyun.openservices.odps.console.xflow;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.XFlows;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/4/14.
 */
public class WaitXFlowCommand extends AbstractCommand {

  private static final Pattern PATTERN = Pattern.compile("\\s*WAIT\\s+(.*)",
                                                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private final String id;

  private WaitXFlowCommand(String id, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.id = id;
  }

  public static WaitXFlowCommand parse(String cmd, ExecutionContext ctx) throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    if (m.matches()) {
      String id = m.group(1);
      Odps odps = OdpsConnectionFactory.createOdps(ctx);
      XFlows xFlows = odps.xFlows();
      boolean isXFlowInstance = xFlows.isXFlowInstance(odps.instances().get(id));
      if (isXFlowInstance) {
        return new WaitXFlowCommand(id, cmd, ctx);
      } else {
        return null;
      }
    }
    return null;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    PAICommand.waitForCompletion(getCurrentOdps().instances().get(id), getCurrentOdps(), getContext());
  }

}

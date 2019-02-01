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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nizheming on 15/4/14.
 */
public class WaitCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"wait", "instance"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: wait [<instanceID>]");
  }

  private static final Pattern PATTERN = Pattern.compile("\\s*WAIT\\s+(.*)",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PATTERN_WITHOUT_ID = Pattern.compile("\\s*WAIT\\s*",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private final String id;

  private WaitCommand(String id, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.id = id;
  }

  public static WaitCommand parse(String cmd, ExecutionContext ctx) {
    Matcher m = PATTERN.matcher(cmd);
    if (m.matches()) {
      String id = m.group(1);
      return new WaitCommand(id, cmd, ctx);
    } else {
      Matcher m2 = PATTERN_WITHOUT_ID.matcher(cmd);
      if (m2.matches() && ctx.isInteractiveQuery()) {
        return new WaitCommand(null, cmd, ctx);
      }
    }

    return null;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    ExecutionContext context = getContext();
    if (id != null) {
      Instance instance = odps.instances().get(id);
      InstanceRunner runner = new InstanceRunner(odps, instance, context);
      runner.waitForCompletion();
      String queryResult = runner.getResult();

      DefaultOutputWriter writer = context.getOutputWriter();

      if (queryResult != null && !queryResult.trim().equals("")) {
        writer.writeResult(queryResult);
      }
    } else {
      Session session = ExecutionContext.getSessionInstance();
      session.printLogView();
    }
  }

}

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
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.Session;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Created by nizheming on 15/4/14.
 */
public class WaitCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"wait", "instance"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: wait [<instanceID>] [-hooks]");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*WAIT\\s+(.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PATTERN_WITHOUT_ID = Pattern.compile(
      "\\s*WAIT\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final String id;
  private final Boolean triggerHooks;

  private WaitCommand(String id, boolean triggerHooks, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.id = id;
    this.triggerHooks = triggerHooks;
  }

  public static WaitCommand parse(String cmd, ExecutionContext ctx) {
    cmd = cmd.trim();

    Matcher m = PATTERN_WITHOUT_ID.matcher(cmd);
    if (m.matches() && ctx.isInteractiveQuery()) {
      return new WaitCommand(null, false, cmd, ctx);
    }

    m = PATTERN.matcher(cmd);
    if (m.matches()) {
      String argString = m.group(1).trim();
      Options options = getOptions();
      String[] args = argString.trim().split("\\s+");
      CommandLineParser parser = new DefaultParser();

      try {
        CommandLine commandLine = parser.parse(options, args);
        String[] unparsedArgs = commandLine.getArgs();
        if (unparsedArgs.length == 1) {
          Boolean triggerHooks = commandLine.hasOption("hooks");
          return new WaitCommand(unparsedArgs[0], triggerHooks, cmd, ctx);
        }
      } catch (ParseException e) {
        return null;
      }
    }

    return null;
  }

  public static Options getOptions() {
    Option hooks = Option
        .builder()
        .longOpt("hooks")
        .argName("trigger hooks")
        .hasArg(false)
        .desc("trigger hooks when instance is terminated")
        .build();

    Options options = new Options();
    options.addOption(hooks);

    return options;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    ExecutionContext context = getContext();
    if (id != null) {
      Instance instance = odps.instances().get(id);
      InstanceRunner runner = new InstanceRunner(odps, instance, context);
      runner.waitForCompletion();

      try {
        String queryResult = runner.getResult();
        DefaultOutputWriter writer = context.getOutputWriter();
        if (queryResult != null && !"".equals(queryResult.trim())) {
          writer.writeResult(queryResult);
        }
      } finally {
        if (triggerHooks) {
          OdpsHooks hooks = new OdpsHooks();
          hooks.after(instance, odps);
        }
      }
    } else {
      Session session = ExecutionContext.getSessionInstance();
      session.printLogView();
    }
  }

}

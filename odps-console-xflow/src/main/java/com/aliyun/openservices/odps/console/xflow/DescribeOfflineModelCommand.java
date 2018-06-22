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

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.OfflineModel;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class DescribeOfflineModelCommand extends AbstractCommand {

  public static final String[]
      HELP_TAGS =
      new String[]{"describe", "desc", "offline", "model", "offlinemodel"};

  private String projectName;
  private String modelName;

  public static void printUsage(PrintStream stream) {
    stream.println(
        "Usage: describe|desc offlinemodel [-p,-project <project_name>] <offlinemodel_name>");
    stream.println("       describe|desc offlinemodel [<project_name>.]<offlinemodel_name>");
  }

  public DescribeOfflineModelCommand(String projectName, String modelName, String cmd,
                                     ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.modelName = modelName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    if (!(odps.offlineModels().exists(projectName, modelName))) {
      throw new ODPSConsoleException("Offlinemodel not found : " + modelName);
    }
    OfflineModel model = odps.offlineModels().get(projectName, modelName);
    printModelInfo(getWriter(), model);
  }

  public static void printModelInfo(DefaultOutputWriter writer, OfflineModel model) {
    writer.writeResult("\n+-------------------------------------------------+");
    writer.writeResult("| Name: " + model.getName());
    writer.writeResult("| Project: " + model.getProject());
    writer.writeResult("| Owner: " + model.getOwner());
    writer.writeResult("| CreateTime: " + ODPSConsoleUtils.formatDate(model.getCreatedTime()));
    writer.writeResult("| LastModifiedTime: " + ODPSConsoleUtils.formatDate(model.getLastModifiedTime()));
    if (model.getType() != null) {  // in case of service is old and console is new
      writer.writeResult("| Type: " + model.getType());
    }
    writer.writeResult("+-------------------------------------------------+\n");
    System.out.flush();
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("p", "project", true, "user spec project");
    return options;
  }

  private static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    try {
      GnuParser parser = new GnuParser();
      return parser.parse(getOptions(), args);
    } catch (ParseException e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " " + e.getMessage(), e);
    }
  }


  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+OFFLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static DescribeOfflineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(2);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;

    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    if (commandLine.getArgList().size() != 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Model name not found.");
    }

    String modelName = commandLine.getArgs()[0];
    if (!modelName.matches("[.\\w]+")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    }

    if (modelName.contains(".")) {
      String[] result = modelName.split("\\.", 2);
      if (projectName != null && (!result[0].equals(projectName))) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Project name conflict.");
      }
      projectName = result[0];
      modelName = result[1];

    }

    if (projectName == null) {
      projectName = ctx.getProjectName();
    }

    return new DescribeOfflineModelCommand(projectName, modelName, cmd, ctx);

  }

}

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
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class ReadOfflineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"read", "offline", "model", "offlinemodel"};

  private ExecutionContext ctx;
  private String projectName;
  private String modelName;
  private String volumeName;
  private String partitionName;

  public static void printUsage(PrintStream stream) {
    stream.println(
        "Usage: read offlinemodel [-p,-project <project_name>] <offlinemodel_name> [as <volume_name>[.<partition>]]");
    stream.println(
        "       read offlinemodel [<project_name>.]<offlinemodel_name> [as <volume_name>[.<partition>]]");
  }

  public ReadOfflineModelCommand(String projectName, String modelName,
                                 String volumeName, String partitionName,
                                 String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.ctx = ctx;
    this.projectName = projectName;
    this.modelName = modelName;
    this.volumeName = volumeName;
    this.partitionName = partitionName;
  }

  public void SubmitWriteVolumeJob() throws OdpsException, ODPSConsoleException {
    getWriter().writeError("Begin write model to volume");
    String algoName = "modeltransfer";
    String format = "pmml";
    String paiCmdStr = "";
    if (partitionName != null) {
      paiCmdStr =
          String.format(
              "pai -name %s -project algo_public -DmodelName=%s -DvolumeName=%s -Dpartition=%s -Dformat=%s",
              algoName, modelName, volumeName, partitionName, format);
    } else {
      paiCmdStr =
          String.format(
              "pai -name %s -project algo_public -DmodelName=%s -DvolumeName=%s -Dformat=%s",
              algoName, modelName, volumeName, format);
    }

    try {
      PAICommand PAICmd = PAICommand.parse(paiCmdStr, ctx);
      PAICmd.run();
    } catch (ODPSConsoleException oce) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    }
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    if (!(odps.offlineModels().exists(projectName, modelName))) {
      throw new ODPSConsoleException("Offlinemodel not found : " + modelName);
    }

    if (volumeName != null) {
      SubmitWriteVolumeJob();
      return;
    }

    // print model content
    OfflineModel model = odps.offlineModels().get(projectName, modelName);
    getWriter().writeResult(model.getModel());
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

  private static Pattern PATTERN = Pattern.compile("\\s*READ\\s+OFFLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ReadOfflineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    if (!m.matches()) {
      return null;
    }

    String input = m.group(1);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    // get -p option if exist
    String projectName = null;
    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    int argSize = commandLine.getArgList().size();
    if (argSize != 1 && argSize != 3) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Model name not found.");
    }

    String modelName = null;
    // read offlinemodel model_name
    modelName = commandLine.getArgs()[0];
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

    String volumeName = null;
    String partitionName = null;
    if (commandLine.getArgList().size() == 3) {
      // read offlinemodel model_name as volume_name
      if (commandLine.getArgs()[1].toUpperCase().compareTo("AS") != 0) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
      }
      volumeName = commandLine.getArgs()[2];
      if (volumeName.contains(".")) {
        String[] result = volumeName.split("\\.", 2);
        volumeName = result[0];
        partitionName = result[1];
      }
    }

    if (projectName == null) {
      projectName = ctx.getProjectName();
    }

    return new ReadOfflineModelCommand(projectName, modelName, volumeName, partitionName, cmd, ctx);
  }

}

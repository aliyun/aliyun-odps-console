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
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.ModelAbTestConf;
import com.aliyun.odps.ml.ModelAbTestInfo;
import com.aliyun.odps.ml.ModelAbTestItem;
import com.aliyun.odps.ml.OnlineModel;
import com.aliyun.odps.ml.OnlineModels;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class UpdateOnlineModelAbtestCommand extends AbstractCommand {

  public static final String[]
      HELP_TAGS =
      new String[]{"update", "online", "model", "onlinemodel", "abtest", "updateabtest"};

  private String projectName;
  private String modelName;
  private ModelAbTestInfo modelInfo;

  public static void printUsage(PrintStream stream) {
    stream
        .println("Usage: updateabtest onlinemodel [-p,-project <project_name>] <onlinemodel_name>");
    stream.println(
        "                                -targetProject <target_model_project> -targetModel <target_model_name> -percentage <percentage(0-100)>");
  }

  public UpdateOnlineModelAbtestCommand(ModelAbTestInfo modelInfo, String cmd,
                                        ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = modelInfo.project;
    this.modelName = modelInfo.modelName;
    this.modelInfo = modelInfo;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    OnlineModels onlinemodels = new OnlineModels(odps.getRestClient());
    if (!onlinemodels.exists(projectName, modelName)) {
      throw new ODPSConsoleException("Onlinemodel not found: " + modelName);
    }
    OnlineModel model = onlinemodels.get(projectName, modelName);
    model.update(modelInfo);
    DescribeOnlineModelCommand.PrintModelInfo(getWriter(), model);
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("p", "project", true, "user spec project");
    options.addOption("targetProject", true, "target onlinemodel's project");
    options.addOption("targetModel", true, "target onlinemodel's name");
    options.addOption("percentage", true, "abtest percentage for target model");
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

  private static Pattern PATTERN = Pattern.compile("\\s*UPDATEABTEST\\s+ONLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static UpdateOnlineModelAbtestCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(1);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;
    ModelAbTestInfo modelInfo = new ModelAbTestInfo();

    if (commandLine.hasOption("p")) {
      modelInfo.project = commandLine.getOptionValue("p");
    }

    if (commandLine.hasOption("targetProject") &&
        commandLine.hasOption("targetModel") &&
        commandLine.hasOption("percentage")) {
      ModelAbTestItem item = new ModelAbTestItem();
      item.project = commandLine.getOptionValue("targetProject");
      item.targetModel = commandLine.getOptionValue("targetModel");
      item.Pct = commandLine.getOptionValue("percentage");
      ModelAbTestConf abTestConf = new ModelAbTestConf();
      abTestConf.items = new ArrayList<ModelAbTestItem>();
      abTestConf.items.add(item);
      modelInfo.abTestConf = abTestConf;
    } else if (!commandLine.hasOption("targetProject") &&
               !commandLine.hasOption("targetModel") &&
               !commandLine.hasOption("percentage")) {
      //clean abTest
      ModelAbTestConf abTestConf = new ModelAbTestConf();
      modelInfo.abTestConf = abTestConf;
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                     + "invalid parameter for onlinemodel abtest, please HELP ONLINEMODEL.");
    }

    if (commandLine.getArgList().size() != 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Model name not found.");
    }

    String modelName = commandLine.getArgs()[0];
    if (!modelName.matches("[\\w]+")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    }
    modelInfo.modelName = modelName;
    if (modelInfo.project == null) {
      modelInfo.project = ctx.getProjectName();
    }
    return new UpdateOnlineModelAbtestCommand(modelInfo, cmd, ctx);
  }
}

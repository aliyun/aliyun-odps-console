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
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.OfflineModel;
import com.aliyun.odps.ml.OfflineModelFilter;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class ShowOfflineModelsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS =
      new String[]{"show", "offline", "model", "offlinemodels", "offlinemodel"};

  private String projectName = null;
  private String modelNamePrefix = null;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show offlinemodels [-p,-project <project_name>] [offlinemodel_name_prefix]");
  }

  public ShowOfflineModelsCommand(String projectName, String modelNamePrefix, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.modelNamePrefix = modelNamePrefix;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    OfflineModelFilter filter = null;

    if (modelNamePrefix != null) {
      filter = new OfflineModelFilter();
      filter.setName(modelNamePrefix);
    }

    long size = 0;
    Iterator<OfflineModel> models = odps.offlineModels().iterator(projectName, filter);
    while(models.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();
      OfflineModel model = models.next();
      getWriter().writeResult(model.getName());
      ++size;
    }
    getWriter().writeResult("\n" + size + " offlinemodels");
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

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(SHOW||LIST)\\s+OFFLINEMODELS($|\\s+(.*))", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ShowOfflineModelsCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }
    Matcher matcher = PATTERN.matcher(cmd);

    if (!matcher.matches()) {
      return null;
    }

    String input = matcher.group(2);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;

    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    if (commandLine.getArgList().size() > 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    }

    String modelNamePrefix = null;
    if (commandLine.getArgList().size() == 1) {
      modelNamePrefix = commandLine.getArgs()[0];
      if (!modelNamePrefix.matches("[.\\w]+")) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid model prefix.");
      }
    }

    if (projectName == null) {
      projectName = ctx.getProjectName();
    }

    ShowOfflineModelsCommand command = new ShowOfflineModelsCommand(projectName, modelNamePrefix, cmd, ctx);
    return command;
  }
}

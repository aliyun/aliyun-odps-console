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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.OfflineModelInfo;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import org.jline.reader.UserInterruptException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CopyOfflineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"copy", "offline", "model", "offlinemodel"};

  private String projectName;
  private OfflineModelInfo modelInfo;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: copy offlinemodel -src_model <src_model_name> -dest_model <dest_model_name> ");
    stream.println("                         [-src_project <src_project_name> -dest_project <dest_project_name>]");
  }

  public CopyOfflineModelCommand(String projectName,
                                 OfflineModelInfo modelInfo,
                                 String cmd,
                                 ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.modelInfo = modelInfo;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    Instance instance = odps.offlineModels().copy(projectName, modelInfo);
    getWriter().writeError("ID = " + instance.getId());
    getWriter().writeError(ODPSConsoleUtils.generateLogView(odps, instance, getContext()));

    try {
      waitForTerminated(instance, 2 * 1000);
    } catch (UserInterruptException e) {
      getWriter().writeError("Instance running background.");
      getWriter().writeError("Use \'kill " + instance.getId() + "\' to stop this instance.");
      throw e;
    }
  }

  private void waitForTerminated(Instance instance, long intervalMs) throws OdpsException {
    try {
      SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      while (!instance.isTerminated()) {
          Thread.sleep(intervalMs);
          getWriter().writeError(sim.format(new Date()) + "\tCopying");
      }

      if (instance.isSuccessful()) {
        getWriter().writeError("OK");
      } else {
        for (Map.Entry<String, Instance.TaskStatus> e : instance.getTaskStatus().entrySet()) {
          if (e.getValue().getStatus() == Instance.TaskStatus.Status.FAILED) {
            throw new OdpsException(instance.getTaskResults().get(e.getKey()));
          } else if (e.getValue().getStatus() != Instance.TaskStatus.Status.SUCCESS) {
            throw new OdpsException(e.getKey() + ", Status=" + e.getValue().getStatus());
          }
        }
      }
    } catch (InterruptedException e) {
      throw new UserInterruptException(e.getMessage());
    }
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("src_project", true, "project to copy model from");
    options.addOption("src_model", true, "source model name");
    options.addOption("dest_project", true, "project to copy model into");
    options.addOption("dest_model", true, "destination model name");
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

  private static Pattern PATTERN = Pattern.compile("\\s*COPY\\s+OFFLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static OfflineModelInfo buildOfflineModelInfo(
          String cmd, Pattern PATTERN, ExecutionContext ctx) throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(1);
    String[] inputs = ODPSConsoleUtils.translateCommandline(input);
    CommandLine commandLine = getCommandLine(inputs);

    OfflineModelInfo modelInfo = new OfflineModelInfo();

    if (commandLine.hasOption("src_project")) {
      modelInfo.srcProject = commandLine.getOptionValue("src_project");
    }

    if (commandLine.hasOption("dest_project")) {
      modelInfo.destProject = commandLine.getOptionValue("dest_project");
    }


    if (commandLine.hasOption("src_model")) {
      modelInfo.srcModel = commandLine.getOptionValue("src_model");
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
          + "invalid parameter for offlinemodel, please HELP OFFLINEMODEL.");
    }

    if (commandLine.hasOption("dest_model")) {
      modelInfo.destModel = commandLine.getOptionValue("dest_model");
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
          + "invalid parameter for offlinemodel, please HELP OFFLINEMODEL.");
    }

    modelInfo.modelName = "";
    modelInfo.modelPath = "";

    return modelInfo;
  }

  public static CopyOfflineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    OfflineModelInfo modelInfo = CopyOfflineModelCommand.buildOfflineModelInfo(cmd, PATTERN, ctx);

    if (modelInfo == null) {
      return null;
    }

    return new CopyOfflineModelCommand(ctx.getProjectName(), modelInfo, cmd, ctx);
  }

}

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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.OfflineModel;
import com.aliyun.odps.ml.OfflineModels;
import com.aliyun.odps.ml.OfflineModelInfo;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsole;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;
import org.jline.reader.UserInterruptException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateOfflineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "offline", "model", "offlinemodel"};

  private String projectName;
  private OfflineModelInfo modelInfo;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: create offlinemodel <offlinemodel_name> -modelPath <model_path> [-arn <oss_role_arn>]");
    stream.println("                           [-type <type> -version <version> -configuration <user_config>]");
    stream.println("       create offlinemodel <offlinemodel_name> -modelPath <model_path> [-arn <oss_role_arn>]");
    stream.println("                           [-process <processor_json>]");
  }

  public CreateOfflineModelCommand(String projectName,
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

    if (odps.offlineModels().exists(projectName, modelInfo.modelName)) {
      throw new ODPSConsoleException("Offlinemodel already exists : " +
              projectName + "." + modelInfo.modelName);
    }

    Instance instance = odps.offlineModels().create(projectName, modelInfo);
    getWriter().writeError("ID = " + instance.getId());
    getWriter().writeError(ODPSConsoleUtils.generateLogView(odps, instance, getContext()));

    try {
      waitForTerminated(instance, 2 * 1000);
    } catch (UserInterruptException e) {
      getWriter().writeError("Instance running background.");
      getWriter().writeError("Use \'kill " + instance.getId() + "\' to stop this instance.");
      throw e;
    }

    if (odps.offlineModels().exists(projectName, modelInfo.modelName)) {
      OfflineModel model = odps.offlineModels().get(projectName, modelInfo.modelName);
      DescribeOfflineModelCommand.printModelInfo(getWriter(), model);
    } else {
      throw new ODPSConsoleException("Create offlinemodel error: " +
              projectName + "." + modelInfo.modelName);
    }
  }

  private void waitForTerminated(Instance instance, long intervalMs) throws OdpsException {
    try {
      SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      while (!instance.isTerminated()) {
          Thread.sleep(intervalMs);
          getWriter().writeError(sim.format(new Date()) + "\tCreating");
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
    options.addOption("modelPath", true, "user spec oss model path");
    options.addOption("arn", true, "oss rolearn");
    options.addOption("type", true,
            "model type:[tensorflow, caffe, mxnet, ...], builtin processor name");
    options.addOption("version", true, "model version, appear with argument 'type'");
    options.addOption("processor", true, "external user spec processor description");
    options.addOption("configuration", true, "user spec configuration for builtin processor");
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

  private static Pattern PATTERN = Pattern.compile("\\s*CREATE\\s+OFFLINEMODEL\\s+(.+)",
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
    if (commandLine.hasOption("modelPath")) {
      modelInfo.modelPath = commandLine.getOptionValue("modelPath");
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
              + "invalid parameter for offlinemodel, please HELP OFFLINEMODEL.");
    }

    if (commandLine.hasOption("arn")) {
      modelInfo.rolearn = commandLine.getOptionValue("arn");
    }
    if (commandLine.hasOption("type") &&
        !commandLine.hasOption("processor")) {
      modelInfo.type = commandLine.getOptionValue("type");
      if (commandLine.hasOption("version")) {
        modelInfo.version = commandLine.getOptionValue("version");
      }

      if (commandLine.hasOption("configuration")) {
        modelInfo.configuration = commandLine.getOptionValue("configuration");
      }
    } else if (commandLine.hasOption("processor") &&
               !commandLine.hasOption("type")) {
      modelInfo.processor = commandLine.getOptionValue("processor");
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
              + "invalid parameter for offlinemodel, please HELP OFFLINEMODEL.");
    }

    if (commandLine.getArgList().size() != 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Model name is ambiguous.");
    }

    String modelName = commandLine.getArgs()[0];
    if (!modelName.matches("[\\w]+")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    }
    modelInfo.modelName = modelName;
    return modelInfo;
  }

  public static CreateOfflineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    OfflineModelInfo modelInfo = CreateOfflineModelCommand.buildOfflineModelInfo(cmd, PATTERN, ctx);

    if (modelInfo == null) {
      return null;
    }

    return new CreateOfflineModelCommand(ctx.getProjectName(), modelInfo, cmd, ctx);
  }

}

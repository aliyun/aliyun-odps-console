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
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.ModelPipeline;
import com.aliyun.odps.ml.ModelPredictDesc;
import com.aliyun.odps.ml.ModelProcessor;
import com.aliyun.odps.ml.OnlineModel;
import com.aliyun.odps.ml.OnlineModelInfo;
import com.aliyun.odps.ml.OnlineModels;
import com.aliyun.odps.ml.OnlineStatus;
import com.aliyun.odps.ml.Resource;
import com.aliyun.odps.ml.Target;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import org.jline.reader.UserInterruptException;

public class CreateOnlineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "model", "onlinemodel", "online"};

  private String projectName;
  private String modelName;
  private OnlineModelInfo modelInfo;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: create onlinemodel [-p,-project <project_name>] <onlinemodel_name>");
    stream.println(
        "                          [-offlinemodelProject <offlinemodel_project>] -offlinemodelName <offlinemodel_name>");
    stream.println(
        "                          [-serviceTag <service_tag>] [-qos <qos>] [-instanceNum <instance_num>] [-cpu <cpu_num>] [-memory <memory_num>] [-gpu <gpu_num>]");
    stream.println(
        "       create onlinemodel [-p,-project <project_name>] <onlinemodel_name> -id <class_name> -libName <library_name> -target <target_name>");
    stream.println(
        "                          -refResource <resResource(resource/volume) split with , > [-configuration <conf>] [-runtime <Native/Jar>]");
    stream.println(
        "                          [-serviceTag <service_tag>] [-qos <qos>] [-instanceNum <instance_num>] [-cpu <cpu_num>] [-memory <memory_num>] [-gpu <gpu_num>]");
  }

  public CreateOnlineModelCommand(OnlineModelInfo modelInfo, String cmd,
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
    if (onlinemodels.exists(projectName, modelName)) {
      throw new ODPSConsoleException("Onlinemodel already exists : " + modelName);
    }
    OnlineModel model = onlinemodels.create(modelInfo);
    SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    while (model.getStatus() == OnlineStatus.DEPLOYING) {
      System.err.println(sim.format(new Date()) + "\tDeploying");
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        throw new UserInterruptException("interrupted while thread sleep");
      }
      model.reload();
    }
    DescribeOnlineModelCommand.PrintModelInfo(getWriter(), model);
  }

  public static Options getOptions() {
    Options options = new Options();
    options.addOption("p", "project", true, "user spec project");
    options.addOption("offlinemodelProject", true, "offlinemodel's project");
    options.addOption("offlinemodelName", true, "offlinemodel's name");
    options.addOption("qos", true, "quality of service");
    options.addOption("instanceNum", true, "instance number");
    options.addOption("cpu", true, "apply for cpu");
    options.addOption("gpu", true, "apply for gpu");
    options.addOption("memory", true, "apply for memory");
    options.addOption("serviceTag", true, "service tag to deploy");

    options.addOption("id", true, "class name");
    options.addOption("libName", true, "library name");
    options.addOption("refResource", true, "reference resource");
    options.addOption("configuration", true, "configurationg for onlinemodel");
    options.addOption("target", true, "target name");
    options.addOption("runtime", true, "indicator for c++ or java");
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


  private static Pattern PATTERN = Pattern.compile("\\s*CREATE\\s+ONLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static OnlineModelInfo buildOnlineModelInfo(String cmd, Pattern PATTERN,
                                                     ExecutionContext ctx)
      throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(1);
    String[] inputs = ODPSConsoleUtils.translateCommandline(input);
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;
    OnlineModelInfo modelInfo = new OnlineModelInfo();
    modelInfo.resource = new Resource();

    if (commandLine.hasOption("p")) {
      modelInfo.project = commandLine.getOptionValue("p");
    }

    if (commandLine.hasOption("offlinemodelName") &&
        !commandLine.hasOption("id") &&
        !commandLine.hasOption("libName") &&
        !commandLine.hasOption("refResource") &&
        !commandLine.hasOption("target")) {
      modelInfo.offlineModelName = commandLine.getOptionValue("offlinemodelName");
      if (commandLine.hasOption("offlinemodelProject")) {
        modelInfo.offlineProject = commandLine.getOptionValue("offlinemodelProject");
      } else {
        modelInfo.offlineProject = ctx.getProjectName();
      }
    } else if (commandLine.hasOption("id") &&
               commandLine.hasOption("libName") &&
               commandLine.hasOption("refResource") &&
               commandLine.hasOption("target") &&
               !commandLine.hasOption("offlinemodelProject") &&
               !commandLine.hasOption("offlinemodelName")) {
      ModelPredictDesc desc = new ModelPredictDesc();
      ModelPipeline pipeline = new ModelPipeline();
      pipeline.processors = new ArrayList<ModelProcessor>();

      ModelProcessor processor = new ModelProcessor();
      processor.className = commandLine.getOptionValue("id");
      processor.libName = commandLine.getOptionValue("libName");
      processor.refResource = commandLine.getOptionValue("refResource").replace(',', ';');
      if (commandLine.hasOption("configuration")) {
        processor.configuration = commandLine.getOptionValue("configuration");
      }
      pipeline.processors.add(processor);

      desc.target = new Target();
      desc.target.name = commandLine.getOptionValue("target");
      desc.pipeline = pipeline;

      modelInfo.predictDesc = desc;
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                     + "invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    }

    if (commandLine.hasOption("qos")) {
      modelInfo.QOS = Short.parseShort(commandLine.getOptionValue("qos"));
    }
    if (commandLine.hasOption("instanceNum")) {
      modelInfo.instanceNum = Short.parseShort(commandLine.getOptionValue("instanceNum"));
    }
    if (commandLine.hasOption("cpu")) {
      modelInfo.resource.CPU = Integer.parseInt(commandLine.getOptionValue("cpu"));
    }
    if (commandLine.hasOption("gpu")) {
      modelInfo.resource.GPU = Integer.parseInt(commandLine.getOptionValue("gpu"));
    }
    if (commandLine.hasOption("memory")) {
      modelInfo.resource.memory = Long.parseLong(commandLine.getOptionValue("memory"));
    }
    if (commandLine.hasOption("serviceTag")) {
      modelInfo.serviceTag = commandLine.getOptionValue("serviceTag");
    }
    if (commandLine.hasOption("runtime")) {
      modelInfo.runtime = commandLine.getOptionValue("runtime");
      if (!modelInfo.runtime.equals("Jar") && !modelInfo.runtime.equals("Native")) {
        throw new ODPSConsoleException(
            ODPSConsoleConstants.BAD_COMMAND + "Parameter -runtime must be Jar or Native.");
      }
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

  public static CreateOnlineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    OnlineModelInfo modelInfo = CreateOnlineModelCommand.buildOnlineModelInfo(cmd, PATTERN, ctx);
    if (modelInfo == null) {
      return null;
    }
    modelInfo.version = 0;
    if (modelInfo.project == null) {
      modelInfo.project = ctx.getProjectName();
    }
    return new CreateOnlineModelCommand(modelInfo, cmd, ctx);
  }

}

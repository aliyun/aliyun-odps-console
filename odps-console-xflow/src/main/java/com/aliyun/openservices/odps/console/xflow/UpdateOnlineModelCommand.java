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
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ml.OnlineModel;
import com.aliyun.odps.ml.OnlineModelInfo;
import com.aliyun.odps.ml.OnlineModels;
import com.aliyun.odps.ml.OnlineStatus;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

import org.jline.reader.UserInterruptException;

public class UpdateOnlineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"update", "model", "onlinemodel", "online"};

  private String projectName;
  private String modelName;
  private OnlineModelInfo modelInfo;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: update onlinemodel [-p,-project <project_name>] <onlinemodel_name>");
    stream.println(
        "                          [-offlinemodelProject <offlinemodel_project>] -offlinemodelName <offlinemodel_name>");
    stream.println(
        "                          [-serviceTag <service_tag>] [-qos <qos>] [-instanceNum <instance_num>] [-cpu <cpu_num>] [-memory <memory_num>] [-gpu <gpu_num>]");
    stream.println(
        "       update onlinemodel [-p,-project <project_name>] <onlinemodel_name> -id <class_name> -libName <library_name> -target <target_name>");
    stream.println(
        "                          -refResource <resResource(resource/volume) split with , > [-configuration <conf>] [-runtime <Native/Jar>]");
    stream.println(
        "                          [-serviceTag <service_tag>] [-qos <qos>] [-instanceNum <instance_num>] [-cpu <cpu_num>] [-memory <memory_num>] [-gpu <gpu_num>]");
  }

  public UpdateOnlineModelCommand(OnlineModelInfo modelInfo, String cmd,
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
    SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    while (model.getStatus() == OnlineStatus.UPDATING) {
      System.err.println(sim.format(new Date()) + "\tUpdating");
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        throw new UserInterruptException("interrupted while thread sleep");
      }
      model.reload();
    }
    DescribeOnlineModelCommand.PrintModelInfo(getWriter(), model);
  }

  private static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    try {
      GnuParser parser = new GnuParser();
      return parser.parse(CreateOnlineModelCommand.getOptions(), args);
    } catch (ParseException e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " " + e.getMessage(), e);
    }
  }


  private static Pattern PATTERN = Pattern.compile("\\s*UPDATE\\s+ONLINEMODEL\\s+(.+)",
                                                   Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static UpdateOnlineModelCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    if (cmd == null || ctx == null) {
      return null;
    }

    OnlineModelInfo modelInfo = CreateOnlineModelCommand.buildOnlineModelInfo(cmd, PATTERN, ctx);
    if (modelInfo == null) {
      return null;
    }

    if (modelInfo.project == null) {
      modelInfo.project = ctx.getProjectName();
    }
    return new UpdateOnlineModelCommand(modelInfo, cmd, ctx);
  }
}

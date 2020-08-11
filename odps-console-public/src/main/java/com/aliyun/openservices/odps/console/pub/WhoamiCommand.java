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

import java.io.PrintStream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * @author shuman.gansm
 * */
public class WhoamiCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"whoami", "who"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: whoami");
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();
    String result = sm.runQuery(getCommandText(), false);
    cOutWhoami(result);
  }

  protected void cOutWhoami(String jsonResult) throws ODPSConsoleException {

    try {
      JsonObject js = new JsonParser().parse(jsonResult).getAsJsonObject();

      if (js.has("DisplayName")) {
        getWriter().writeResult("Name: " + js.get("DisplayName").getAsString());
      }
      if (js.has("SourceIP")) {
        getWriter().writeResult("Source IP: " + js.get("SourceIP").getAsString());
      }
      if (js.has("VpcId")) {
        getWriter().writeResult("VPC ID: " + js.get("VpcId").getAsString());
      }

      getWriter().writeResult("End_Point: " + getContext().getEndpoint());
      if (!StringUtils.isNullOrEmpty(getContext().getTunnelEndpoint())) {
        getWriter().writeResult("Tunnel_End_Point: " + getContext().getTunnelEndpoint());
      }
      getWriter().writeResult("Project: " + getContext().getProjectName());

    } catch (JsonParseException e) {
      throw new ODPSConsoleException("parse whoami error:" + e.getMessage());
    }
  }

  public WhoamiCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static WhoamiCommand parse(String commandString, ExecutionContext sessionContext) {

    // 不处理set语句
    if (commandString.trim().toUpperCase().equals("WHOAMI")) {
      return new WhoamiCommand(commandString, sessionContext);
    }

    return null;
  }
}

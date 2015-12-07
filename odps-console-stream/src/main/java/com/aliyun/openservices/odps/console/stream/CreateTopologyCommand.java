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

package com.aliyun.openservices.odps.console.stream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.FileUtil;

import java.io.PrintStream;
import java.util.regex.*;

public class CreateTopologyCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "add", "topology"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: create topology <topolotyname> meta <descpath> comment <comment>");
  }

  private String topologyName = "";
  private String comment = "";
  private String descPath = "";

  public CreateTopologyCommand(String commandText, ExecutionContext context, String topologyName, String comment, String descPath) {
    super(commandText, context);
    this.topologyName = topologyName;
    this.comment = comment;
    this.descPath = descPath;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    String topologyDescription = FileUtil.getStringFromFile(descPath);
    odps.topologies().create(topologyName, topologyDescription);
    outputWriter.writeResult("OK");
  }

  public static CreateTopologyCommand parse(String commandString, ExecutionContext sessionContext) {
    assert (commandString != null);
    String tempString = commandString.toUpperCase();

    String topologyName = "";
    String descPath = "";
    String comment = "";
    boolean nextIsComment = false;

    String params[] = commandString.trim().split("\\s+");
    String pattern = "\\s*CREATE\\s+TOPOLOGY\\s+[a-zA-Z0-9_]+\\s+META\\s+[^\\s]+((\\s+COMMENT\\s+\'[^\']*\')|(\\s+COMMENT\\s+\"[^\"]*\")){0,1}";
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(tempString);
    if (m.matches())
    {
        topologyName = params[2];
        descPath = params[4];
        if (m.group(1) != null) {
            String commentStr = m.group(1);
            comment = commentStr.substring(commentStr.indexOf("COMMENT") + 7);
            comment = comment.trim();
        }

        return new CreateTopologyCommand(commandString, sessionContext, topologyName, comment, descPath);
    }
    return null;
  }
}

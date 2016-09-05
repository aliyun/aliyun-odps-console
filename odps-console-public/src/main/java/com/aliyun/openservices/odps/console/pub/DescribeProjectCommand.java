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
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * Created by nizheming on 15/4/15.
 */
public class DescribeProjectCommand extends AbstractCommand {

  private final String projectName;
  private boolean extended;

  public DescribeProjectCommand(
      String projectName, boolean extended, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.extended = extended;
  }

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "project"};

  // Each subtype of desc command is not visible in CommandParserUtils, so group all help info here together
  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc project [-extended] <projectname>");
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("extended", false, "need extended properties");
    return options;
  }

  public static AbstractCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    if (cmd == null || ctx == null) {
      return null;
    }

    String[] tokens = new AntlrObject(cmd).getTokenStringArray();

    if (tokens.length < 2) {
      return null;
    }

    if (!("DESC".equalsIgnoreCase(tokens[0]) || "DESCRIBE".equalsIgnoreCase(tokens[0]))) {
      return null;
    }

    if (!("PROJECT".equalsIgnoreCase(tokens[1]))) {
      return null;
    }

    CommandLine parser = null;
    try {
      parser = new GnuParser().parse(getOptions(), tokens);
    } catch (ParseException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    }
    boolean extended = parser.hasOption("extended");
    if (parser.getArgList().size() != 3) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    String projectName = parser.getArgs()[2];

    return new DescribeProjectCommand(projectName, extended, cmd, ctx);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    Project prj = odps.projects().get(projectName);
    prj.reload();

    ExecutionContext context = getContext();
    PrintWriter out = new PrintWriter(System.out);
    out.printf("%-40s%-40s\n", "Name", prj.getName());
    out.printf("%-40s%-40s\n", "Description", prj.getComment());
    out.printf("%-40s%-40s\n", "Owner", prj.getOwner());
    out.printf("%-40s%-40s\n", "CreatedTime", prj.getCreatedTime());

    Map<String, String> properties = prj.getProperties();
    if (properties != null) {
      out.println("\nProperties:");
      for (Map.Entry<String, String> e : properties.entrySet()) {
        out.printf("%-40s%-40s\n", e.getKey(), e.getValue());
      }
    }

    if (extended) {
      Map<String, String> extendedProperties = prj.getExtendedProperties();
      out.println("\nExtended Properties:");
      if (extendedProperties != null) {
        for (Map.Entry<String, String> e : extendedProperties.entrySet()) {
          out.printf("%-40s%-40s\n", e.getKey(), e.getValue());
        }
      }
    }


    out.flush();
    //Security Configuration use Show Security Configuration
    //Not Display in Desc Project
  }
}

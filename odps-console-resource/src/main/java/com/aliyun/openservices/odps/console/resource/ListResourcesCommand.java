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

package com.aliyun.openservices.odps.console.resource;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * list resources
 *
 * @author shuman.gansm
 */
public class ListResourcesCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"list", "ls", "show", "resource", "resources"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: ls|list resources [-p <projectname>] [-l]");
  }

  private String project;
  // showAll mean shows source table of table type resource
  private boolean showAll;

  public ListResourcesCommand(String commandText, ExecutionContext context, String project, boolean showAll) {
    super(commandText, context);

    this.project = project;
    this.showAll = showAll;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", true, "project name");
    Option showAllOption = new Option("l", false, "show source of resource");

    project_name.setRequired(false);
    showAllOption.setRequired(false);

    opts.addOption(project_name);
    opts.addOption(showAllOption);

    return opts;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();

    int[] columnPercent = showAll ?
                          new int[]{25, 15, 15, 10, 10, 25} :
                          new int[]{30, 20, 15, 10, 25};
    String[] resourceTitle = showAll ?
                             new String[] {"Resource Name", "Owner", "Last Modified Time", "Type", "Source", "Comment"} :
                             new String[] {"Resource Name", "Owner", "Last Modified Time", "Type", "Comment"};

    ODPSConsoleUtils.formaterTableRow(resourceTitle, columnPercent, getContext().getConsoleWidth());
    Iterator<Resource> resListing;

    if (project != null) {
      resListing = odps.resources().iterator(project);
    } else {
      resListing = odps.resources().iterator();
    }

    int totalCount = 0;
    int columnCounter = 0;
    for (; resListing.hasNext(); ) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Resource p = resListing.next();
      String[] resourceAttr = new String[columnPercent.length];
      resourceAttr[columnCounter++] = p.getName();
      resourceAttr[columnCounter++] = p.getOwner();
      resourceAttr[columnCounter++] = p.getLastModifiedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
                                                                                                .getLastModifiedTime());
      resourceAttr[columnCounter++] = p.getType() == null ? " " : p.getType().toString().toLowerCase();

      if (showAll) {
        if (p.getType() == Resource.Type.TABLE) {
          TableResource tr = (TableResource) p;
          String tableSource = tr.getSourceTable().getProject() + "." + tr.getSourceTable().getName();
          resourceAttr[columnCounter++] = tableSource;
        } else {
          resourceAttr[columnCounter++] = "";
        }
      }

      resourceAttr[columnCounter++] = p.getComment() == null ? " " : p.getComment();

      ODPSConsoleUtils.formaterTableRow(resourceAttr, columnPercent, getContext().getConsoleWidth());
      totalCount++;
      columnCounter = 0;
    }

    getWriter().writeError(totalCount + " resources");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(LS|LIST)\\s+RESOURCES($|\\s.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ListResourcesCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    Matcher match = PATTERN.matcher(commandString);

    if (match.matches()) {
      Options opts = initOptions();
      CommandLine cl = CommandParserUtils.getCommandLine(commandString, opts);
      return new ListResourcesCommand(commandString, sessionContext, cl.getOptionValue("p"), cl.hasOption("l"));
    }

    return null;

  }

}

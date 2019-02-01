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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import java.io.PrintStream;
import java.util.Iterator;

public class ListProjectsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"list", "ls", "show", "project", "projects"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: list projects");
  }

  public ListProjectsCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    Iterator<Project> projects = odps.projects().iterator(null);
    //check permission
    projects.hasNext();

    String projectTitle[] = { "Project Name", "Comment", "Creation Time", "Last Modified Time" };
    // 设置每一列的百分比
    int columnPercent[] = { 30, 20, 25, 25 };
    int consoleWidth = getContext().getConsoleWidth();

    ODPSConsoleUtils.formaterTableRow(projectTitle, columnPercent, consoleWidth);

    long size = 0;
    for (; projects.hasNext();) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Project p = projects.next();
      String projectAttr[] = new String[4];
      projectAttr[0] = p.getName();
      projectAttr[1] = p.getComment() == null ? " " : p.getComment();
      projectAttr[2] = p.getCreatedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
          .getCreatedTime());
      projectAttr[3] = p.getLastModifiedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
          .getLastModifiedTime());
      ++size;
      ODPSConsoleUtils.formaterTableRow(projectAttr, columnPercent, consoleWidth);
    }

    getWriter().writeError(size + " projects");
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ListProjectsCommand parse(String commandString, ExecutionContext context) {

    if (commandString.toUpperCase().matches("\\s*LIST\\s+PROJECTS\\s*")) {
      return new ListProjectsCommand(commandString, context);
    }

    return null;
  }

}

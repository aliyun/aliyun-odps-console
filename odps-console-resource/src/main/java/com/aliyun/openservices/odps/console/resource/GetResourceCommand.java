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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

public class GetResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"get", "resource", "download"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: get resource [<projectname>:]<resourcename> <path>");
  }

  private static Pattern PATTERN_PREFIX = Pattern.compile("\\s*GET\\s+RESOURCE(.*)",
      Pattern.CASE_INSENSITIVE);
  private static Pattern PATTERN = Pattern.compile("\\s*GET\\s+RESOURCE\\s+(.*)\\s+(.*)",
      Pattern.CASE_INSENSITIVE);
  private String projectName;
  private String resourceName;
  private String path;

  public GetResourceCommand(String projectName, String resourceName, String path, String cmd,
      ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.resourceName = resourceName;
    this.path = path;
  }

  public static GetResourceCommand parse(String cmd, ExecutionContext ctx) throws ODPSConsoleException {
    if (cmd == null || ctx == null) {
      return null;
    }
    
    Matcher prefix = PATTERN_PREFIX.matcher(cmd);
    
    boolean match = prefix.matches();

    if (!match) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    
    match = m.matches();
    
    if (!match) {
      System.err.println("Usage: get resource [<project name>:]<resource name> <path>;");
      throw new ODPSConsoleException("Bad command.");
    }
    
    String resourceName = m.group(1);
    String projectName = ctx.getProjectName();
    if (resourceName.contains(":")) {
      String[] result = resourceName.split(":", 2);
      projectName = result[0];
      resourceName = result[1];
    }

    String path = m.group(2);

    return new GetResourceCommand(projectName, resourceName, path, cmd, ctx);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    Resource resource = odps.resources().get(projectName, resourceName);

    resource.reload();

    if (!(resource instanceof FileResource)) {
      throw new ODPSConsoleException(
          "Resource should be a File Type Resource(file, jar, py, archive)");
    }

    FileResource fileResource = (FileResource) resource;

    File file = new File(path);
    if (file.exists() && file.isDirectory()) {
      file = new File(path + File.separator + fileResource.getName());
    }
    try {
      FileOutputStream os = new FileOutputStream(file);
      InputStream is = odps.resources().getResourceAsStream(projectName, resourceName);
      IOUtils.copy(is, os);
      is.close();
      os.close();
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    }
    System.err.println("OK");
  }
}

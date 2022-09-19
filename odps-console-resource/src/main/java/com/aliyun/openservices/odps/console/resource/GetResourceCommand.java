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
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.FileUtil;

public class GetResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"get", "resource", "download"};

  public static void printUsage(PrintStream out, ExecutionContext ctx) {
    // deprecated usage
    // get resource project:name <path>
    if (ctx.isProjectMode()) {
      out.println("Usage: get resource [<project name>:]<resource name> <path>");
    } else {
      out.println("Usage: get resource [[<project name>:]<schema name>:]<resource name> <path>");
    }
  }

  //todo remove prefix
  private static Pattern PATTERN_PREFIX = Pattern.compile("\\s*GET\\s+RESOURCE(.*)",
                                                          Pattern.CASE_INSENSITIVE);
  //todo (.*?) ???
  private static Pattern PATTERN = Pattern.compile("\\s*GET\\s+RESOURCE\\s+(.*?)\\s+(.*)",
                                                   Pattern.CASE_INSENSITIVE);

  private Coordinate coordinate;
  private String path;

  public GetResourceCommand(Coordinate coordinate, String path, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.coordinate = coordinate;
    this.path = path;
  }

  public static GetResourceCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    if (!PATTERN_PREFIX.matcher(cmd).matches()) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    if (!m.matches()) {
      System.err.println(
          "Usage: get resource [<project name>:[<schema name>:]<resource name> <path>;");
      throw new ODPSConsoleException("Bad command.");
    }

    String resourceName = m.group(1);
    Coordinate coordinate = Coordinate.getCoordinateABC(resourceName, ":");
    String path = FileUtil.expandUserHomeInPath(m.group(2));

    return new GetResourceCommand(coordinate, path, cmd, ctx);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String resourceName = coordinate.getObjectName();

    Odps odps = getCurrentOdps();
    Resource resource = odps.resources().get(projectName, schemaName, resourceName);

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
    FileOutputStream os = null;
    InputStream is = null;
    try {
      os = new FileOutputStream(file);
      is = odps.resources().getResourceAsStream(projectName, schemaName, resourceName);
      IOUtils.copy(is, os);
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    } finally {
      IOUtils.closeQuietly(is);
      IOUtils.closeQuietly(os);
    }
    System.err.println("OK");
  }
}

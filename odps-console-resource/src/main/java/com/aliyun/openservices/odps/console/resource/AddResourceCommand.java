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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.VolumeArchiveResource;
import com.aliyun.odps.VolumeFileResource;
import com.aliyun.odps.VolumeResource;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.FileUtil;

public class AddResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"add", "create", "resource"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: ADD <FILE | ARCHIVE >  [AS alias] [COMMENT 'cmt'][-F];");
    out.println("       ADD TABLE <tablename> [PARTITION (SPEC)] [AS alias] [COMMENT 'cmt'][-F];");
    out.println("       ADD <PY | JAR> <localfile[.py |.jar]> [COMMENT 'cmt'][-F];");
    out.println("       ADD <VOLUMEFILE|VOLUMEARCHIVE> <filename> AS <alias> [COMMENT 'cmt'][-F];");
  }

  String refName;
  String alias;
  String comment;
  String type;
  String partitionSpec;
  String projectName;

  boolean isUpdate;

  public String getRefName() {
    return refName;
  }

  public String getAlias() {
    return alias;
  }

  public String getComment() {
    return comment;
  }

  public String getType() {
    return type;
  }

  public String getPartitionSpec() {
    return partitionSpec;
  }

  public boolean isUpdate() {
    return isUpdate;
  }

  public AddResourceCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public AddResourceCommand(String commandText,
                            ExecutionContext context, String refName, String alias,
                            String comment, String type, String partitionSpec, boolean isUpdate, String projectName) {
    super(commandText, context);
    this.refName = refName;
    this.alias = alias;
    this.comment = comment;
    this.type = type;
    this.partitionSpec = partitionSpec;
    this.isUpdate = isUpdate;
    this.projectName = projectName;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    String aliasSuffix;

    if (StringUtils.isNullOrEmpty(alias)) {
      aliasSuffix = refName.toUpperCase();
    } else {
      aliasSuffix = alias.toUpperCase();
    }

    // file/py/jar/archive
    if (type.equals("PY") && !aliasSuffix.endsWith(".PY")) {
      // 则命令出错
      throw new ODPSConsoleException(ODPSConsoleConstants.FILENAME_ENDWITH_PY);
    }
    if (type.equals("JAR") && !aliasSuffix.endsWith(".JAR")) {
      // 则命令出错
      throw new ODPSConsoleException(ODPSConsoleConstants.FILENAME_ENDWITH_JAR);
    }

    // .jar/.zip/.tgz/.tar.gz/.tar
    if (type.endsWith("ARCHIVE")
        && !(aliasSuffix.endsWith(".JAR")
             || aliasSuffix.endsWith(".ZIP") || aliasSuffix.endsWith(".TGZ")
             || aliasSuffix.endsWith(".TAR.GZ") || aliasSuffix.endsWith(".TAR"))) {
      // 则命令出错
      throw new ODPSConsoleException(ODPSConsoleConstants.FILENAME_ENDWITH_MORE);
    }

    if (!StringUtils.isNullOrEmpty(partitionSpec) && StringUtils.isNullOrEmpty(alias)) {
      // 如果出现partition (spec)，则必须带alias
      throwUsageError(System.err, "pls set alias.");
    }

    Odps odps = getCurrentOdps();
    if (this.projectName == null || this.projectName.isEmpty()) {
      this.projectName = odps.getDefaultProject();
    }

    FileResource resource = null;

    Type resType = Type.valueOf(type.toUpperCase());
    switch (resType) {
      case VOLUMEFILE:
      case VOLUMEARCHIVE:
        addVolume(odps);
        break;
      case ARCHIVE:
        resource = new ArchiveResource();
        break;
      case PY:
        resource = new PyResource();
        break;
      case JAR:
        resource = new JarResource();
        break;
      case FILE:
        resource = new FileResource();
        break;
      case TABLE:
        addTable(odps);
        break;
    }
    if (resource != null) {
      addFile(odps, resource);
    }
    if (!isUpdate) {
      getWriter().writeError("OK: Resource '" + alias + "' have been created.");
    } else {
      getWriter().writeError("OK: Resource '" + alias + "' have been updated.");
    }


  }

  private void addFile(Odps odps, FileResource resource) throws OdpsException, ODPSConsoleException {
    refName = FileUtil.expandUserHomeInPath(refName);

    File file = new File(refName);
    if (file.exists()) {

      // 如果没有设置as alias, 则默认为文件名
      if (alias == null || alias.length() == 0) {
        alias = file.getName();
      }

      resource.setName(alias);
      resource.setComment(comment);

      FileInputStream inputStream = null;
      try {
        inputStream = new FileInputStream(file);
        if (!isUpdate) {
          odps.resources().create(projectName, resource, inputStream);

        } else {
          try {
            odps.resources().update(projectName, resource, inputStream);

          } catch (NoSuchObjectException e) {
            // 先要把打开的流关闭，reset没有用
            inputStream.close();
            inputStream = new FileInputStream(file);
            odps.resources().create(projectName, resource, inputStream);

          }
        }

      } catch (IOException e) {

        throw new ODPSConsoleException(ODPSConsoleConstants.FILE_UPLOAD_FAIL, e);
      } finally {

        if (inputStream != null) {
          try {
            inputStream.close();
          } catch (IOException e) {
          }
        }
      }

    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.FILE_NOT_EXIST + ":" + refName);
    }
  }

  private void addTable(Odps odps) throws OdpsException, ODPSConsoleException {
    String tablePartition = refName;
    PartitionSpec spec = null;
    if (partitionSpec == null || partitionSpec.length() == 0) {
      partitionSpec = null;
    } else {
      spec = new PartitionSpec(partitionSpec);
    }

    TableResource resource = new TableResource(refName, null, spec);
    resource.setComment(comment);
    resource.setName(alias);

    if (!isUpdate) {
      odps.resources().create(projectName, resource);
    } else {

      try {
        odps.resources().update(projectName, resource);
      } catch (NoSuchObjectException e) {
        // 如果不存在,则直接添加
        odps.resources().create(projectName, resource);
      }
    }
  }

  private void addVolume(Odps odps) throws OdpsException, ODPSConsoleException {

    if ("".equals(alias)) {
      throwUsageError(System.err, "pls set alias for volume file.");
    }

    String refPath = projectName + "/volumes" + (refName.startsWith("/") ? "" : "/") + refName;

    Type resType = Type.valueOf(type.toUpperCase());

    VolumeResource resource = null;

    if (resType == Type.VOLUMEFILE) {
      resource = new VolumeFileResource();
    } else if (resType == Type.VOLUMEARCHIVE){
      resource = new VolumeArchiveResource();
    } else {
      throw new ODPSConsoleException("unsupported volume resource type: " + resType);
    }

    resource.setComment(comment);
    resource.setName(alias);
    resource.setVolumePath(refPath);

    if (isUpdate) {
      try {
        odps.resources().update(resource);
        return;
      } catch (NoSuchObjectException e) {
      }
    }

    odps.resources().create(resource);
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  private static final Pattern TO_PACKAGE_REGEX = Pattern.compile("\\s*ADD.*TO\\s+PACKAGE.*\\s*",
                                                                  Pattern.CASE_INSENSITIVE);

  public static boolean isSecurityCommand(String cmd) {
    if (cmd == null || cmd.length() == 0) {
      return false;
    }

    boolean r = false;
    Matcher m = TO_PACKAGE_REGEX.matcher(cmd);
    if (m != null && m.matches()) {
      r = true;
    }
    return r;
  }

  public static AddResourceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    /*
     * !HACK: filter ADD <object type> <object name> TO PACKAGE <package name>
     * [WITH PRIVILEGES privileges];
     */
    if (isSecurityCommand(commandString)) {
      return null;
    }

    String oldString = commandString;
    // file/py/jar/archive
    if (commandString.toUpperCase().matches(
        "\\s*ADD\\s+((FILE)|(PY)|(JAR)|(ARCHIVE)|(TABLE)|(VOLUMEFILE)|(VOLUMEARCHIVE))[\\s\\S]*")) {

      commandString = commandString.replaceAll("\\s+", " ").trim();

      // 把add去掉
      commandString = commandString.substring(4);
      boolean isUpdate = false;
      if (commandString.toUpperCase().endsWith(" -F")) {
        isUpdate = true;
        commandString = commandString.substring(0, (commandString.length() - 2)).trim();
      }

      int commandIndex = commandString.indexOf(" ");
      if (commandIndex == -1) {
        throwUsageError(System.err, "bad command");
      }

      // 取出命令file/py/jar/archive
      String command = commandString.substring(0, commandIndex).trim().toUpperCase();
      commandString = commandString.substring(commandIndex).trim();

      int fileIndex = commandString.indexOf(" ");
      String refName;
      if (fileIndex == -1) {
        // 在此，可能有一些非法字符， 要判断localFile的正确性
        refName = commandString;
      } else {
        refName = commandString.substring(0, fileIndex);
      }

      commandString = commandString.substring(refName.length()).trim();

      String partitionSpec = "";
      // 如果command是table，则要先处理partition_spec
      if (command.equals("TABLE")) {

        if (commandString.toUpperCase().indexOf("PARTITION") == 0 && commandString.indexOf("(") > 0
            && commandString.indexOf(")") > 0) {
          partitionSpec = commandString.substring(commandString.indexOf("(") + 1,
                                                  commandString.indexOf(")"));

          commandString = commandString.substring(commandString.indexOf(")") + 1).trim();
        }

      }

      // as alias
      int asIndex = commandString.toUpperCase().indexOf("AS ");
      String alias = "";
      if (asIndex == 0) {
        // 说明有as，不是add table的情况
        commandString = commandString.substring(2).trim();

        int asIndexSpace = commandString.indexOf(" ");
        if (asIndexSpace > 0) {
          alias = commandString.substring(0, asIndexSpace);
        } else {
          alias = commandString;
        }
      }
      commandString = commandString.substring(alias.length()).trim();

      // comment
      int commentIndex = commandString.toUpperCase().indexOf("COMMENT ");
      String comment = "";

      if (commentIndex == 0) {
        // 说明有as，不是add table的情况
        commandString = commandString.substring("COMMENT ".length()).trim();
        comment = commandString;
      }
      commandString = commandString.substring(comment.length()).trim();

      if (!commandString.isEmpty()) {
        String warningName = "AddResourceCommandAsMissing";
        Long count = OdpsDeprecatedLogger.getDeprecatedCalls().get(warningName);
        if (count == null) {
          count = 1L;
        } else {
          count += 1L;
        }
        OdpsDeprecatedLogger.getDeprecatedCalls().put(warningName, count);

        System.err.println(
            "Warning: ignore part \"" + commandString + "\" in command, maybe 'AS' is missing");
      }

      if (command.equals("TABLE") && (alias == null || alias.length() == 0)) {
        alias = refName;
      }

      AddResourceCommand addResourceCommand = new AddResourceCommand(oldString, sessionContext);
      addResourceCommand.type = command;
      // refName和alias都可以引号括引来
      addResourceCommand.refName = refName.replaceAll("'", "").replaceAll("\"", "");
      addResourceCommand.alias = alias.replaceAll("'", "").replaceAll("\"", "");
      addResourceCommand.comment = comment;
      addResourceCommand.partitionSpec = partitionSpec.replaceAll(" ", "");
      addResourceCommand.isUpdate = isUpdate;
      return addResourceCommand;
    }

    return null;
  }

  private static void throwUsageError(PrintStream out, String msg) throws ODPSConsoleException {
    printUsage(out);
    throw new ODPSConsoleException(msg);
  }
}

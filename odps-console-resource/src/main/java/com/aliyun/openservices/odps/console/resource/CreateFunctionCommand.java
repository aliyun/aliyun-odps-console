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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.aliyun.odps.Function;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;


public class CreateFunctionCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "add", "function"};
  private boolean isUpdate;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: create function <functionname> as '<classname>' using '<res>,...'");
  }

  String functionName;
  String className;
  List<String> useList;

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public List<String> getUseList() {
    return useList;
  }

  public CreateFunctionCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    Function function = new Function();
    

    // 设定依赖库
    List<String> resources = new ArrayList<String>();
    resources.addAll(useList);

    
    function.setName(functionName);
    function.setClassPath(className);
    function.setResources(resources);

    if (isUpdate) {
      try {
        odps.functions().update(function);
        getWriter().writeError("Success: Function '" + functionName + "' have been updated.");
        return;
      } catch (NoSuchObjectException e) {
      }
    }
    odps.functions().create(function);
    getWriter().writeError("Success: Function '" + functionName + "' have been created.");

  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static CreateFunctionCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // file/py/jar/archive
    if (commandString.toUpperCase().matches("\\s*CREATE\\s+FUNCTION[\\s\\S]*")) {

      String functionName = "";
      String className = "";
      String using = "";

      commandString = commandString.replaceAll("\\s+", " ").trim();


      Pattern pattern = Pattern.compile("\\s+using\\s+", Pattern.CASE_INSENSITIVE);

      String[] usingArray = pattern.split(commandString);
      boolean isUpdate = false;
      // 两段
      if (usingArray.length == 2) {
        using = usingArray[1];
        String[] funCommandArray = usingArray[0].split(" ");
        if (using.endsWith(" -f")) {
          using = using.substring(0, using.lastIndexOf(" -f"));
          isUpdate=true;
        }

        if (funCommandArray.length != 5) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Too Many Arguments.");
        }

        if (!funCommandArray[3].toUpperCase().equals("AS")) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Lack of keyWord 'as'." );
        }

        functionName = funCommandArray[2];
        className = funCommandArray[4];

      } else {
        // bad
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Lack of 'using' sentense.");
      }

      CreateFunctionCommand createFunctionCommand = new CreateFunctionCommand(commandString,
          sessionContext);
      createFunctionCommand.className = className.replaceAll("'", "").replaceAll("\"", "");
      createFunctionCommand.functionName = functionName;

      createFunctionCommand.useList = Arrays.asList(using.replaceAll("'", "").replaceAll("\"", "")
          .replaceAll(" ", "").split(","));
      createFunctionCommand.isUpdate = isUpdate;

      return createFunctionCommand;
    }

    return null;
  }

}

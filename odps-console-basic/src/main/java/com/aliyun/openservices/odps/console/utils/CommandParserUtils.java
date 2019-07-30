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

package com.aliyun.openservices.odps.console.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;

import com.aliyun.odps.OdpsDeprecatedLogger;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.CompositeCommand;
import com.aliyun.openservices.odps.console.commands.InstancePriorityCommand;
import com.aliyun.openservices.odps.console.commands.InteractiveCommand;
import com.aliyun.openservices.odps.console.commands.LoginCommand;
import com.aliyun.openservices.odps.console.commands.UseProjectCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * 命令行解析、包括非交互模式、交互模式
 *
 * @author shuman.gansm
 */
public class CommandParserUtils {

  private static final String COMMAND_PACKAGE = "com.aliyun.openservices.odps.console.commands";

  /**
   * 支持命令行模式的Command，如果加入新的Command实现，按相应顺序加入到数组的指定位置。
   * 注意：ExecuteCommand是-e执行的，如果需要在-e之前执行，需要放在ExecuteCommand之前
   */
  private static final String[] BASIC_COMMANDS =
      new String[]{"SetEndpointCommand", "LoginCommand", "UseProjectCommand", "DryRunCommand",
                   "MachineReadableCommand", "FinanceJsonCommand",
                   "AsyncModeCommand", "InstancePriorityCommand", "SkipCommand", "SetRetryCommand",
                   "InteractiveCommand", "ExecuteCommand", "ExecuteFileCommand",
                   "ExecuteScriptCommand", "HelpCommand", "ShowVersionCommand",
                   "UseProjectCommand", "SetCommand", "UnSetCommand", "HistoryCommand",
                   "ArchiveCommand", "MergeCommand"
      };

  private static final String HELP_TAGS_FIELD = "HELP_TAGS";
  private static final String HELP_PRINT_METHOD = "printUsage";

  public static String[] getCommandTokens(String cmd) throws ODPSConsoleException {
    AntlrObject antlr = new AntlrObject(cmd);
    String[] args = antlr.getTokenStringArray();

    if (args == null) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }
    return args;
  }

  public static CommandLine getCommandLine(String[] args, Options opts)
      throws ODPSConsoleException {

    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  public static CommandLine getCommandLine(String cmd, Options opts) throws ODPSConsoleException {
    return getCommandLine(getCommandTokens(cmd), opts);
  }

  /**
   * 解析命令行模式的参数。
   *
   * @param args
   * @return
   * @throws ODPSConsoleException
   */
  public static AbstractCommand parseOptions(String args[], ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // 为option list赋值，optionlist保存了需要处理的所有参数
    List<String> optionList = new LinkedList<String>(Arrays.asList(args));

    // 处理参数为-e"", --project=等情况，转换成标准的option输入
    optionList = populateOptions(optionList);
    removeHook(optionList, sessionContext);

    // commandList保存了parse出来的所有command集合
    List<AbstractCommand> commandList = new LinkedList<AbstractCommand>();
    parseCommandLineCommand(commandList, optionList, sessionContext);

    // 判断参数是否都已经处理, 如果还有参数没有处理，则抛出异常
    if (optionList.size() > 0) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    boolean hasLoginCommand = false;
    for (AbstractCommand command : commandList) {
      if (command instanceof LoginCommand) {
        hasLoginCommand = true;
        break;
      }
    }
    // 如果设置了account_provider情况下，没有logincomand，添加一个，通过logincomand输入密码
    if (!hasLoginCommand && sessionContext.getAccountProvider() != null) {
      commandList.add(0, new LoginCommand(sessionContext.getAccountProvider(),
                                          sessionContext.getAccessId(), null, null,
                                          "--account_provider", sessionContext));
    }

    checkUseProject(commandList, sessionContext);
    CompositeCommand compositeCommand = new CompositeCommand(commandList, "", sessionContext);

    return compositeCommand;
  }

  public static void removeHook(List<String> optionList, ExecutionContext sessionContext) {

    // 去掉hook不能够用command来实现，只能把option在这里去掉，并设置context
    if (optionList.contains("--enablehook")) {

      if (optionList.indexOf("--enablehook") + 1 < optionList.size()) {

        int index = optionList.indexOf("--enablehook");
        // 创建相应的command列表
        String hook = optionList.get(index + 1);

        // 消费掉--enablehook 及参数
        optionList.remove(optionList.indexOf("--enablehook"));
        optionList.remove(optionList.indexOf(hook));

        if (!Boolean.valueOf(hook)) {
          sessionContext.setOdpsHooks(null);
        }
      }
    }

  }

  /**
   * 解析交互模式的命令。
   *
   * @param commandLines
   * @return
   * @throws ODPSConsoleException
   */
  public static AbstractCommand parseCommand(String commandLines, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    commandLines = commandLines.trim();
    if (commandLines.equals("")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.INVALID_PARAMETER_E);
    }

    int index = commandLines.lastIndexOf(";");
    if (index != commandLines.length() - 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.COMMAND_END_WITH);
    }

    OdpsDeprecatedLogger.getDeprecatedCalls().put("USER_COMMANDS :" + commandLines, 1L);

    // 命令行，可支持多个命令
    List<String> commandLinelist = new AntlrObject(commandLines).splitCommands();

    // 如果SqlLinesParser解析出来为空，且与“--”开始，是引号不匹配,这种场景,把query发给相应的command自己处理
    if (commandLinelist.size() == 0 && !commandLines.trim().startsWith("--")) {

      commandLinelist.add(commandLines.substring(0, index));
    }

    // 从命令行得到多个命令列表
    List<AbstractCommand> odpsCommandList = new LinkedList<AbstractCommand>();

    // query number
    int i = 0;
    // 如果大于command的计数，则skip掉
    int step = sessionContext.getStep();
    for (String command : commandLinelist) {

      i++;
      if (step > i) {
        continue;
      }

      command = command.trim();
      // for null command
      if (command.equals("")) {
        continue;
      }

      parseInteractiveCommand(odpsCommandList, command, sessionContext, i);
    }

    // 如果大于query数，则抛出错误
    if (step > i) {
      throw new ODPSConsoleException(
          "[Error] invalid NUM for option k, total query count inlcude empty query: " + i);
    }

    if (odpsCommandList.size() == 1) {
      return odpsCommandList.get(0);
    } else {
      CompositeCommand compositeCommand = new CompositeCommand(odpsCommandList, "", sessionContext);

      return compositeCommand;
    }
  }

  private static void checkUseProject(List<AbstractCommand> commandList,
                                      ExecutionContext sessionContext) throws ODPSConsoleException {
    if (sessionContext.getProjectName() == null || sessionContext.getProjectName().equals("")) {
      return;
    }
    boolean useProjectFlag = false;
    for (AbstractCommand command : commandList) {
      if (command instanceof UseProjectCommand || command instanceof InteractiveCommand) {
        useProjectFlag = true;
      }
    }
    if (!useProjectFlag) {
      String commandText = "--project=" + sessionContext.getProjectName();
      UseProjectCommand useProjectCommand = new UseProjectCommand(sessionContext.getProjectName(),
                                                                  commandText, sessionContext);
      int index = 0;
      boolean loginCommandExist = false;
      for (AbstractCommand command : commandList) {
        index++;
        if (command instanceof LoginCommand) {
          loginCommandExist = true;
          break;
        }

      }
      if (loginCommandExist) {
        commandList.add(index, useProjectCommand);
      } else {
        commandList.add(0, useProjectCommand);
      }
    }

    // 因为第一次启动，use project可能把设置的priorty值给设置为默认值
    // 所以需要用初始值创建一个 InstancePriorityCommand
    boolean ipcExist = false;
    int useCommandIndex = 0;
    for (int i = 0; i < commandList.size(); i++) {
      AbstractCommand command = commandList.get(i);
      if (command instanceof InstancePriorityCommand) {
        ipcExist = true;
      } else if (command instanceof UseProjectCommand) {
        useCommandIndex = i + 1;
      }
    }
    if (!ipcExist) {
      // 如果不存在，则需要加一个InstancePriorityCommand来设置Priority的值,存在就不管了
      InstancePriorityCommand ipc = new InstancePriorityCommand(sessionContext.getPriority(),
                                                                "--instance_priority",
                                                                sessionContext);
      commandList.add(useCommandIndex, ipc);
    }

  }

  private static void addCommand(List<AbstractCommand> commandList, AbstractCommand command,
                                 ExecutionContext sessionContext) {

    if (command != null) {

      // command.setContext(sessionContext);
      commandList.add(command);
    }
  }

  private static final long COMMAND_DEFAULT_PRIORITY = 100000;


  public static List<PluginPriorityCommand> getExtendedCommandList() {
    if (extendedCommandList == null) {
      extendedCommandList = PluginUtil.getExtendCommandList();

      for (int i = 0; i < BASIC_COMMANDS.length; i++) {
        String commandName = COMMAND_PACKAGE + "." + BASIC_COMMANDS[i];
        extendedCommandList
            .add(new PluginPriorityCommand(commandName, COMMAND_DEFAULT_PRIORITY - i));
      }


      Collections.sort(extendedCommandList);
    }
    return extendedCommandList;
  }

  static List<PluginPriorityCommand> extendedCommandList;

  public static Set<String> getAllCommandKeyWords() {
    Set<String> keys = new HashSet<String>();

    List<PluginPriorityCommand> ecList = getExtendedCommandList();

    for (PluginPriorityCommand command : ecList) {
      try {
        Class<?> commandClass = getClassFromPlugin(command.getCommandName());
        Field tags_field = commandClass.getField(HELP_TAGS_FIELD);
        keys.addAll(Arrays.asList((String[]) tags_field.get(null)));

      } catch (NoSuchFieldException e) {
        // Ignore
      } catch (IllegalAccessException e) {
        // Ignore
      }
    }

    return keys;
  }

  public static void printHelpInfo(List<String> keywords) {
    Map<String, Integer> matched = new HashMap<String, Integer>() {
    };
    List<String> allCommands = new ArrayList<String>();

    List<PluginPriorityCommand> ecList = getExtendedCommandList();

    for (PluginPriorityCommand command : ecList) {
      allCommands.add(command.getCommandName());
    }

    if (classLoader == null) {
      loadPlugins();
    }

    for (String commandName : allCommands) {
      try {
        Class<?> commandClass = getClassFromPlugin(commandName);
        Field tags_field = commandClass.getField(HELP_TAGS_FIELD);
        String[] tags = (String[]) tags_field.get(null);

        int count = 0;
        for (String tag : tags) {
          for (String keyword : keywords) {
            if (tag.equalsIgnoreCase(keyword)) {
              count++;
              break;
            }
          }
        }
        if (count != 0) {
          matched.put(commandName, count);
        }
      } catch (NoSuchFieldException e) {
        // Ignore
      } catch (IllegalAccessException e) {
        // Ignore
      }
    }

    // Print the help info of the command(s) whose tags match most keywords
    boolean found = false;
    System.out.println();
    for (int i = keywords.size(); i >= 1; i--) {
      for (Map.Entry<String, Integer> entry : matched.entrySet()) {
        if (i == entry.getValue()) {
          try {
            Class<?> commandClass = getClassFromPlugin(entry.getKey());
            Method printMethod = commandClass.getDeclaredMethod(HELP_PRINT_METHOD,
                                                                new Class<?>[]{PrintStream.class});
            printMethod.invoke(null, System.out);
            found = true;
          } catch (NoSuchMethodException e) {
            // Ignore
          } catch (IllegalAccessException e) {
            // Ignore
          } catch (InvocationTargetException e) {
            // Ignore
          }
        }
      }
      if (found) {
        break;
      }
    }
    System.out.println();
  }

  /**
   * parseCommandLine 按顺序分别 解析对应的 CommandLine
   * 其中会把符合条件的 optionList 消化掉,然后交给 InteractiveCommand 或者 ExeucteCommand
   * 这时候再根据剩下的 option来决定到底是什么模式的命令.
   *
   * @param commandList
   * @param optionList
   * @param sessionContext
   * @throws ODPSConsoleException
   */
  private static void parseCommandLineCommand(List<AbstractCommand> commandList,
                                              List<String> optionList,
                                              ExecutionContext sessionContext)
      throws ODPSConsoleException {

    List<PluginPriorityCommand> ecList = getExtendedCommandList();

    for (PluginPriorityCommand command : ecList) {
      String commandName = command.getCommandName();
      if (commandName != null && !"".equals(commandName.trim())) {
        AbstractCommand cmd = null;
        try {
          cmd = reflectCommandObject(commandName, new Class<?>[]{List.class,
                                                                 ExecutionContext.class},
                                     optionList, sessionContext);
        } catch (AssertionError e) {
          // 如果用户类加载不了,console不直接退出，只输出相应信息
          sessionContext.getOutputWriter().writeDebug(e.getMessage());
          System.err.println("fail to load user command, pls check:" + commandName);
        }

        if (cmd != null) {
          addCommand(commandList, cmd, sessionContext);
        }
      }
    }
  }

  private static void parseInteractiveCommand(List<AbstractCommand> commandList,
                                              String commandText, ExecutionContext sessionContext,
                                              int queryNumber)
      throws ODPSConsoleException {

    List<PluginPriorityCommand> ecList = new ArrayList<PluginPriorityCommand>();
    ecList.addAll(getExtendedCommandList());
    // 加载用户定义的类，用户定义的类如果没有找到，console不会失败直接退出

    String userCommands = sessionContext.getUserCommands();

    if (userCommands != null) {
      // 为了保证用户定义命令的优先使用，将其命令优先级权重设为最高。
      for (String commandString : Arrays.asList(userCommands.split(","))) {
        ecList.add(new PluginPriorityCommand(commandString, PluginPriorityCommand.MAX_PRIORITY));
      }
    }

    Collections.sort(ecList);

    for (PluginPriorityCommand command : ecList) {
      String commandName = command.getCommandName();
      if (commandName != null && !"".equals(commandName.trim())) {
        AbstractCommand cmd = null;
        try {
          cmd = reflectCommandObject(commandName, new Class<?>[]{String.class,
                                                                 ExecutionContext.class},
                                     commandText, sessionContext);
        } catch (AssertionError e) {
          // 如果用户类加载不了,console不直接退出，只输出相应信息
          sessionContext.getOutputWriter().writeDebug(e.getMessage());
          System.err.println("fail to load user command, pls check:" + commandName);
        }

        if (cmd != null) {
          cmd.setCommandStep(queryNumber);
          addCommand(commandList, cmd, sessionContext);
          return;
        }
      }
    }
  }

  static URLClassLoader classLoader;

  public static void loadPlugins() {
    List<URL> pluginJarList = PluginUtil.getPluginsJarList();
    URL[] urls = (URL[]) pluginJarList.toArray(new URL[pluginJarList.size()]);
    classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
  }

  private static Class<? extends AbstractCommand> getClassFromPlugin(String commandName) {
    if (classLoader == null) {
      loadPlugins();
    }

    try {
      Class<? extends AbstractCommand> commandClass = (Class<? extends AbstractCommand>) Class.forName(commandName, false, classLoader);
      return commandClass;
    } catch (ClassNotFoundException e) {
      // 在Console代码正确的情况下不应该出现该异常，所以抛出AssertionError。
      throw new AssertionError("Cannot find the command:" + commandName);
    }
  }

  private static AbstractCommand reflectCommandObject(String commandName, Class<?>[] argTypes,
                                                      Object... args) throws ODPSConsoleException {

    Class<?> commandClass = null;
    try {

      commandClass = getClassFromPlugin(commandName);
      Method parseMethod = commandClass.getDeclaredMethod("parse", argTypes);

      Object commandObject = parseMethod.invoke(null, args);

      if (commandObject != null) {
        return (AbstractCommand) commandObject;
      } else {
        return null;
      }
    } catch (SecurityException e) {
      throw new AssertionError("Cannot find the parse method on the command: " + commandName);
    } catch (NoSuchMethodException e) {
      //FOR there's two kind of command,not throw exception
      return null;
    } catch (IllegalArgumentException e) {
      throw new AssertionError("Failed to invoke the parse method on the command:" + commandName);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Failed to invoke the parse method on the command:" + commandName);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof ODPSConsoleException) {
        String msg = e.getCause().getMessage();
        if (!StringUtils.isNullOrEmpty(msg) && msg.contains(ODPSConsoleConstants.BAD_COMMAND)
            && commandClass != null) {
          String output = getCommandUsageString(commandClass);
          if (output != null) {
            throw new ODPSConsoleException(e.getCause().getMessage() + "\n" + output);
          }
        }
        throw (ODPSConsoleException) e.getCause();
      } else {
        throw new ODPSConsoleException(e.getCause());
      }
    }
  }

  private static String getCommandUsageString(Class<?> commandClass) {
    try {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(os);
      Method printMethod = commandClass.getDeclaredMethod("printUsage",
                                                          new Class<?>[]{PrintStream.class});
      printMethod.invoke(null, ps);
      return os.toString("UTF8");

    } catch (Exception e) {
      return null;
    }
  }

  // 处理参数为-e"", --project=等情况，转换成标准的option输入
  private static List<String> populateOptions(List<String> optionList) {

    List<String> resultList = new LinkedList<String>();

    for (String optionStr : optionList) {

      if (!StringUtils.isNullOrEmpty(optionStr)) {
        String option = optionStr.trim();

        if (option.matches("^--[^= ]+ *= *[^ ]+")) {

          // ^--[^-= ]+ *= *[^ ]+
          String[] cmds = option.split("=", 2);

          // 把命令转换成command可以识别的
          if (cmds[0].trim().equals("--username")) {
            resultList.add("-u");
          } else if (cmds[0].trim().equals("--password")) {
            resultList.add("-p");
          } else {
            resultList.add(cmds[0].trim());
          }

          resultList.add(cmds[1].trim());

        } else if (option.matches("-[a-z].+")) {

          resultList.add(option.substring(0, 2));
          resultList.add(option.substring(2));

        } else {
          resultList.add(option);
        }
      }
    }

    return resultList;
  }

  /**
   * odpscmd 的运行参数 (例如: -u xxx -p xxx) 已经被隐藏, 使用 -I filepath 替换了.
   * 真正的运行参数被存放在 filepath 这个文件中, 需从中读取, 返回真正的命令参数
   *
   * @throws ODPSConsoleException
   */
  public static String[] getCommandArgs(String[] args) throws ODPSConsoleException {
    if (args.length == 2 && args[0].trim().equalsIgnoreCase("-I")) {
      return getArgsFromTempFile(args[1]);
    } else {
      return args;
    }
  }

  private static String[] getArgsFromTempFile(String file) throws ODPSConsoleException
  {
    String params = readFromFile(file);

    List<String> optionList = new ArrayList<String>();
    if (!StringUtils.isNullOrEmpty(params)) {
      String[] options = params.split("\0");

      for (String opt : options) {
        if (!StringUtils.isNullOrEmpty(opt)) {
          optionList.add(opt);
        }
      }
    }

    return optionList.toArray(new String[0]);
  }

  private static String readFromFile(String fileName) throws ODPSConsoleException
  {
    File file = null;
    try {
      file = new File(fileName);
      return FileUtils.readFileToString(file, "utf-8");
    } catch (IOException e) {
      throw new ODPSConsoleException("read args file error: " + e.getMessage() , e);
    }
    finally {
      FileUtils.deleteQuietly(file);
    }
  }
}

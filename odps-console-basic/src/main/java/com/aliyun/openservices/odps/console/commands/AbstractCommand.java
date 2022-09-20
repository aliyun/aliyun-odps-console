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

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.jsoup.nodes.Document;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * 所有command，从此抽象类来实现,包括交互式 command 和 非交互式 command.
 * 若需要实现 XXXCommand, 则须从此类继承, 并实现以下部分:
 *  1.静态成员变量 <code> public static String[] HELP_TAGS; </code>, 是此命令的标记字符串数组.例如 DescribeTableCommand 的 HELP_TAGS 可以是 {"desc", "table"};
 *  2.静态方法 <code> public static void printUsage(PrintStream stream); </code> , 用于输出命令 Usage 信息;
 *  3.静态方法 <code> public static XXXCommand parse(...) </code>, 用于解析输入,生成对应的 XXXCommand. 交互式与非交互式 command 在实现该方法时使用的参数有所不同:
 *    - 交互式 command 须实现
 *        <code> public static XXXCommand parse(String commandString, ExecutionContext sessionContext); </code>
 *    - 非交互式 command 须实现
 *        <code> public static XXXCommand parse(List<String> optionList, ExecutionContext sessionContext); </code>
 *  4.抽象接口 {@link #run()}, 包含命令运行的逻辑.
 *
 *  command 的实现可参考示例 {@link HelpCommand}, 它既是交互式也是非交互式 command, 因此实现了两种 parse 方法.
 *
 * @author shuman.gansm
 * */

/*
 * 2种类型的Command
 * 交互式 parse(String, context) 执行-e 和 -f
 * 非交互式 parse(List<String> Option, context)
 */
public abstract class AbstractCommand {

  // 每个命令会保存对应的上下文件
  private ExecutionContext context;
  // 每个命令都有对应的命令行
  // 命令解析出来后，需要set一下命令行
  private String commandText;

  // default
  int commandStep = -1;

  public int getCommandStep() {
    return commandStep;
  }

  public void setCommandStep(int commandStep) {
    this.commandStep = commandStep;
  }

  public AbstractCommand(String commandText, ExecutionContext context) {
    super();
    this.context = context;
    this.commandText = commandText;
  }

  protected abstract void run() throws OdpsException, ODPSConsoleException;

  public void execute() throws OdpsException, ODPSConsoleException {
    // run hooked command first
    String clzName = this.getClass().getSimpleName();
    String hook = ExecutionContext.commandBeforeHook.get(clzName);
    if (hook != null) {
      if (getContext().isDebug()) {
        getWriter().writeError("before hook of command " + clzName + ": " + hook);
      }
      try {
        Constructor c = CommandParserUtils.getClassFromPlugin(hook)
            .getConstructor(String.class, ExecutionContext.class);
        AbstractCommand cmd = (AbstractCommand) c.newInstance(getCommandText(), getContext());
        if (getContext().isDebug()) {
          getWriter().writeError("invoking hook: " + hook);
        }
        cmd.execute();
      } catch (NoSuchMethodException | IllegalAccessException |
          InstantiationException | InvocationTargetException e) {
        if (getContext().isDebug()) {
          e.printStackTrace();
        }
      }
    }

    // run self
    if (getContext().isDebug()) {
      getWriter().writeError("invoking command: " + this.getClass().getName());
    }
    run();
  }

  public ExecutionContext getContext() {
    assert (context != null);

    return context;
  }

  public String getCommandText() {
    return commandText;
  }

  public DefaultOutputWriter getWriter() {

    ExecutionContext context = this.getContext();
    return context.getOutputWriter();
  }

  /**
   * 创建当前session对应的project
   * */
  public String getCurrentProject() throws ODPSConsoleException {

    ExecutionContext context = this.getContext();
    if (context == null) {
      throw new ODPSConsoleException(ODPSConsoleConstants.EXECUTIONCONTEXT_NOT_BE_SET);
    }

    String project = context.getProjectName();
    if (project == null || "".equals(project.trim())) {
      throw new ODPSConsoleException(ODPSConsoleConstants.PROJECT_NOT_BE_SET);
    }

    return project;

  }
  
  /**
   * 创建当前session对应的Odps
   * @throws ODPSConsoleException 
   * */
  public Odps getCurrentOdps() throws ODPSConsoleException {
    //todo createOdps set default schema??
    return OdpsConnectionFactory.createOdps(getContext());
  }

  public void setContext(ExecutionContext context) {
    this.context = context;
  }

  public static final String[] HELP_TAGS = new String[]{};

  /**
   * Print the usage of command
   * Each
   */
  public static void printUsage(PrintStream stream) {
    stream.println("This command have no help info");
  }
}

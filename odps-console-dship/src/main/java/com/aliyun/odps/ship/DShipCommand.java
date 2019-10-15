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

package com.aliyun.odps.ship;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ship.common.CommandType;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import org.jline.reader.UserInterruptException;

/**
 * Created by lulu on 15-3-4.
 */
public class DShipCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"tunnel", "dship"};

  public static void printUsage(PrintStream out) {
    try {
      DShip.showHelp("help.txt");
    } catch (IOException e) {
      out.println("Error: IO Error when get help info of tunnel");
    }
  }

  public DShipCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public static DShipCommand parse(String commandString, ExecutionContext sessionContext) {
    String readCommandString = commandString;
    if (readCommandString.trim().equalsIgnoreCase("TUNNEL") ||
        readCommandString.toUpperCase().matches("\\s*TUNNEL\\s+.*")) {
      readCommandString = readCommandString.trim().replaceAll("\\s+", " ");
      return new DShipCommand(readCommandString, sessionContext);
    }
    return  null;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    String commandString = this.getCommandText();

    //TRICKY
    //这里有三类配置参数，优先级由高到低依次是：dship commandline配置，odpsconsole commandline 配置和配置文件
    //其中odpsconsole commandline 配置和配置文件的内容保存在ExecutionContext中，在这里无法区分分别来自哪里
    //这里把ExecutionContext的内容也放在dship commandline的最前面，有两个目的：
    // 1 保证ExecutionContext的配置覆盖配置文件的内容，从而保证odpsconsole commandline 配置优先级高于配置文件
    // 2 保证dship commandline的配置覆盖ExecutionContext，从而保证dship commandline 配置优先级高于console commandline
    // 所以dship args结构是<subcommand> + <ExecutionContext args> + <dship commandline args>

    // split dship command into "<dship> <subcommand> <suffix>"
    List<String> _args = new ArrayList<String>(Arrays.asList(commandString.split(" ", 3)));
    _args.remove(0); // remove leading dship
    if (_args.size() == 0) {
      _args.add("help"); // if no subcommand provided, default use help
    }

    // fill execution context as leading args
    String[] suffix;
    // split suffix as following args
    if (_args.size() == 2) {
      suffix = ODPSConsoleUtils.translateCommandline(_args.get(1));
      _args.remove(1);
      _args.addAll(Arrays.asList(suffix));
    }

    DshipContext.INSTANCE.setExecutionContext(getContext());

    String[] args = _args.toArray(new String[_args.size()]);
    CommandType type = DShip.parseSubCommand(args);
    try {
      DShip.runSubCommand(type, args);
    } catch (UserInterruptException e) {
      throw e;
    } catch (Throwable throwable) {
      throw new ODPSConsoleException("error occurred while running tunnel command", throwable);
    }
  }

}

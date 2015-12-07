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
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class CreateResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "add", "resource"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: create resource <type> <refname> [[<(spec)>)] <alias>] ");
    stream.println("       [-p,-project <projectname>] [-c,-comment <comment>] [-f,-force]");
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    // Do Nothing
  }

  public CreateResourceCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public static AddResourceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String[] tokens = new AntlrObject(commandString).getTokenStringArray();

    if (tokens != null && tokens.length >= 2 &&
        tokens[0].toUpperCase().equals("CREATE") && tokens[1].toUpperCase().equals("RESOURCE")) {

      GnuParser parser = new GnuParser();
      Options options = new Options();
      options.addOption("p", "project", true, null);
      options.addOption("c", "comment", true, null);
      options.addOption("f", "force", false, null);

      try {
        CommandLine cl = parser.parse(options, tokens);

        String refName = null;
        String alias = "";
        String comment = null;
        String type = null;
        String partitionSpec = "";
        boolean isUpdate = false;

        List<String> argList = cl.getArgList();
        int size = argList.size();

        if (size < 4) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Missing parameters");
        }

        ListIterator<String> iter = argList.listIterator();
        iter.next();
        iter.next();
        type = iter.next();
        refName = iter.next();
        if (iter.hasNext()) {
          String item = iter.next();
          if (item.equals("(")) {
            boolean isParenPaired = false;

            while (iter.hasNext()) {
              String s = iter.next();
              if (s.equals(")")) {
                isParenPaired = true;
                break;
              }
              partitionSpec += s;
            }

            if (!isParenPaired) {
              throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Unpaired parenthesis");
            }

            if (!iter.hasNext()) {
              throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Missing parameter: alias");
            }
            item = iter.next();
          }

          alias = item;
        }

        if (iter.hasNext()) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Illegal parameter: " + iter.next());
        }

        String projectName = null;
        Option[] opts = cl.getOptions();
        for (Option opt : opts) {
          if ("f".equals(opt.getOpt())) {
            isUpdate = true;
          } else if ("c".equals(opt.getOpt())) {
            comment = opt.getValue();
          } else if ("p".equals(opt.getOpt())) {
            projectName = opt.getValue();
          } else {
            throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Illegal option: " + opt.getOpt());
          }
        }

        return new AddResourceCommand(commandString, sessionContext,
                                      refName, alias, comment, type, partitionSpec, isUpdate,
                                      projectName);
      } catch (ParseException e) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid parameters");
      }
    } else {
      return null;
    }
  }
}

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

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.openservices.odps.console.utils.*;
import org.fusesource.jansi.Ansi;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.InPlaceUpdates;

/**
 * 处理交互模式
 */
public class InteractiveCommand extends AbstractCommand {

  public static boolean quit = false;

  public static boolean isInteractiveMode = false;

  private ODPSConsoleReader consoleReader = null;

  @SuppressWarnings("restriction")
  public void run() throws OdpsException, ODPSConsoleException {

    // 设定交互模式
    isInteractiveMode = true;
    // 交互模式没有step的说法
    getContext().setStep(0);
    getContext().setInteractiveMode(isInteractiveMode);

    // 欢迎版本信息
    getWriter().writeError(ODPSConsoleConstants.LOGO);
    getWriter().writeError(ODPSConsoleConstants.ALIYUN_ODPS_UTILITIES_VERSION);

    checkUpdate();

    // window下用sconner来读取command，其它的都用jline来处理,因为jline在window下处理不好输入。
    consoleReader = ODPSConsoleUtils.getOdpsConsoleReader();

    String inputStr = "";
    String endPoint = getContext().getEndpoint();
    if (StringUtils.isNullOrEmpty(endPoint)) {
      throw new ODPSConsoleException("Failed: endpoint cannot be null or empty.");
    }

    // 初始的交互模式前缀
    String prefix = "odps" + "@ " + getContext().getProjectName();

    if (getContext().getProjectName() != null && !getContext().getProjectName().isEmpty()) {
      String projectName = getContext().getProjectName();

      System.err.println(String.format("Connecting to %s, project: %s", endPoint, projectName));
      try {
        String commandText = "use project " + projectName;
        UseProjectCommand useProjectCommand = new UseProjectCommand(projectName, commandText, getContext());
        useProjectCommand.run();
        System.err.println("Connected!");
      }
      catch (Exception ex) {
        System.err.println("Accessing project '" + projectName + "' failed: " + ex.getMessage());
      }
    }

    if (getContext().getAutoSessionMode()) {
      SessionUtils.autoAttachSession(getContext(), getCurrentOdps());
    }

    // q;会退出，还有一种情况，ctrl+d时inputStr返回null
    while (inputStr != null) {
      // 和declient一样，在交互模式下，忽略注释行
      if (!StringUtils.isNullOrEmpty(inputStr) && !inputStr.trim().startsWith("--")) {
        // 如果输入不为空，则解析命令并执行
        try {
          // drop command, need confirm
          if (isConfirm(inputStr)) {
            AbstractCommand command =
                CommandParserUtils.parseCommand(inputStr, this.getContext());
            command.execute();
          }

          if (quit) {
            break;
          }
        } catch (UserInterruptException e) {
          // isConfirm may throw too
          inputStr = "";
        } catch (Exception e) {
          String extraMsg = "";
          if (e instanceof OdpsException) {
             extraMsg = String.format(" [ RequsetId: %s ]. ", ((OdpsException) e).getRequestId());
          }
          getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + e.getMessage() + extraMsg);
          if (StringUtils.isNullOrEmpty(e.getMessage())) {
            getWriter().writeError(StringUtils.stringifyException(e));
          }
          getWriter().writeDebug(e);
        }
        prefix = "odps@ " + getContext().getProjectName();

      }

      if (getContext().getProjectName() != null) {
        // inputStr will end with ';', but there may be multiple '\n' before ';'
        // e.g. show tables\n\n\n;
        inputStr = consoleReader.readLine(prefix + ODPSConsoleConstants.IDENTIFIER);
      }
    }

    // 退出后，换一行，显示格式更好看一些
    System.err.println();
  }

  /**
   * if return is null, don't need confirm or suggest confirm
   */
  protected String getConfirmInfomation(String commandText) {

    String upCommandText = commandText.trim().toUpperCase();

    if (upCommandText.startsWith("DROP") // resource, function, role
        || upCommandText.startsWith("DELETE") // resource, function
        || upCommandText.startsWith("REMOVE") // resource, user
        || isDropPartitionCmd(upCommandText)) {
      // delete trailing ';' and '\n'
      commandText = commandText.substring(0, commandText.length() - 1).trim();
      return "Confirm to \"" + commandText + "\" (yes/no)? ";
    }

    if (upCommandText.matches("PUT\\s+POLICY\\s+.*")
        || (upCommandText.matches("SET\\s+PROJECTPROTECTION.*")
            && upCommandText.indexOf("EXCEPTION ") > 0)) {
      return "will overwrite the old policy content  (yes/no)? ";
    }

    if (upCommandText.matches("SERVER\\s+DELETE\\s+.*")) {
      return "Confirm to remove server and delete all data permanently (Cannot Undo!). (yes/no)? ";
    }

    return null;
  }

  private boolean isConfirm(String commandText) throws IOException {

    String confirmText = getConfirmInfomation(commandText);
    if (confirmText == null) {
      // don't confirm
      return true;
    }

    String inputStr;
    while (true) {
      inputStr = consoleReader.readConfirmation(confirmText);

      if (inputStr == null) {
        return false;
      }

      inputStr = inputStr.trim().toUpperCase();
      if ("N".equals(inputStr) || "NO".equals(inputStr)) {
        return false;
      } else if ("Y".equals(inputStr) || "YES".equals(inputStr)) {
        return true;
      }
    }
  }

  public InteractiveCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static InteractiveCommand parse(List<String> paraList, ExecutionContext sessionContext) {
    if (paraList.size() == 0) {
      return new InteractiveCommand("", sessionContext);
    }

    if (paraList.contains("-e") && paraList.indexOf("-e") + 1 == paraList.size()) {
      // 如果没有command，让此命令走交互模式
      paraList.remove(paraList.indexOf("-e"));

      return new InteractiveCommand("", sessionContext);
    }

    return null;
  }

  private static Pattern IS_DROP_PARTITION = Pattern.compile(
      "\\s*ALTER\\s+TABLE\\s+[\\w\\.]+\\s+DROP.+", Pattern.CASE_INSENSITIVE);

  static boolean isDropPartitionCmd(String cmd) {
    boolean r = false;
    if (cmd != null) {
      Matcher m = IS_DROP_PARTITION.matcher(cmd);
      r = m.matches();
    }
    return r;
  }

  private void checkUpdate() {
    String updateUrl = getContext().getUpdateUrl();

    if (!StringUtils.isNullOrEmpty(updateUrl)) {
      UpdateChecker checker = new UpdateChecker(updateUrl, getContext());

      if (checker.shouldPromptUpdate()) {
        String message =
            String.format("New version %s available! Try it now! %s", checker.getOnlineVersion(),
                          checker.getOnlineDownloadURL());

        if (InPlaceUpdates.isUnixTerminal()) {
          InPlaceUpdates.reprintLineWithColorAsBold(System.err, message,
                                                    Ansi.Color.CYAN);
        } else {
          getWriter().writeError(message);
        }
      }
    }
  }
}

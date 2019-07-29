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

package com.aliyun.openservices.odps.console;

import com.aliyun.odps.*;
import com.aliyun.odps.data.SessionQueryResult;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dejun.xiedj on 2017/10/27.
 */
public class InteractiveQueryCommand extends MultiClusterCommandBase {

  private static int OBJECT_STATUS_RUNNING = 2;
  private static int OBJECT_STATUS_FAILED = 4;
  private static int OBJECT_STATUS_TERMINATED = 5;
  private static int OBJECT_STATUS_CANCELLED = 6;
  private static String startRegex = "start\\s+session(\\s+\\w+)?";
  private static String useRegex = "use\\s+session\\s+(\\w+)";
  private static String attachRegex = "attach\\s+session\\s+(\\w+)";
  private static String stopRegex = "stop\\s+session";
  private static String stopWithIdRegex = "stop\\s+session\\s+(\\w+)";
  private static String taskName = "console_sqlrt_task";

  private void startSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getSessionInstance() != null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You are already in a session. Session name: " +
          ExecutionContext.getSessionInstance().getSessionName());
      return;
    }

    Session session = Session.create(
        getCurrentOdps(),
        sessionName,
        getCurrentProject(),
        SetCommand.setMap,
        0L
    );
    ExecutionContext.setSessionInstance(session);
    getContext().setInteractiveQuery(true);
  }

  private void attachSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getSessionInstance() != null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You are already in a session. Session name: " +
          ExecutionContext.getSessionInstance().getSessionName());
      return;
    }

    try {
      Session session = Session.attach(
          getCurrentOdps(),
          sessionName,
          SetCommand.setMap,
          0L
      );
      ExecutionContext.setSessionInstance(session);
    } catch (OdpsException ex) {
      getContext().setInteractiveQuery(false);
      throw ex;
    } catch (ODPSConsoleException ex) {
      getContext().setInteractiveQuery(false);
      throw ex;
    }
  }

  private void useSession(String id) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getSessionInstance() != null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You are already in a session.");
      return;
    }

    Odps odps = getCurrentOdps();
    Instance instance = odps.instances().get(id);
    try {
      if (instance.getTaskStatus().get(taskName).getStatus() != Instance.TaskStatus.Status.RUNNING) {
        throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Session " + id + " is not running.");
      } else {
        Session session = new Session(odps, instance);
        ExecutionContext.setSessionInstance(session);
        getContext().setInteractiveQuery(true);
      }
    } catch (NoSuchObjectException ex) {
      getContext().setInteractiveQuery(false);
      throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Session " + id + " is not exist. Try to check the session ID.");
    } catch (Exception ex) {
      getContext().setInteractiveQuery(false);
      throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Use session " + id + " failed. " + ex.toString());
    }
  }

  private void submitSubQuery() throws OdpsException, ODPSConsoleException {
    long startTime = System.currentTimeMillis();
    String commandString = getCommandText();
    if (!commandString.endsWith(";")) {
      commandString = commandString + ";";
    }

    if (ExecutionContext.getSessionInstance() == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }


    HashMap<String, String> settings = new HashMap();
    if (!SetCommand.setMap.isEmpty()) {
      for (Map.Entry<String, String> property : SetCommand.setMap.entrySet()) {
        settings.put(property.getKey(), property.getValue());
      }
    }
    if (!getContext().isMachineReadable()) {
      settings.put("odps.sql.select.output.format", "HumanReadable");
    }

    SessionQueryResult subqueryResult = ExecutionContext.getSessionInstance().run(commandString, settings);
    Iterator<Session.SubQueryResponse> responseIterator = subqueryResult.getResultIterator();
    boolean firstRecord = true;

    while (responseIterator.hasNext()) {
      Session.SubQueryResponse response = responseIterator.next();
      if (!StringUtils.isNullOrEmpty(response.warnings)) {
        getWriter().writeResult("Warnings: " + response.warnings);
      }

      if (response.status == OBJECT_STATUS_FAILED) {
        throw new ODPSConsoleException(response.result);
      } else if (response.status != OBJECT_STATUS_RUNNING) {
        getWriter().writeResult(response.result);

        String postMessage = "";
        if (!commandString.matches(startRegex)) {
          postMessage += "Session sub-query-id: " + String.valueOf(response.subQueryId) + ", ";
        }
        postMessage += "cost time: " + String.valueOf(System.currentTimeMillis() - startTime) + " ms.";
        getWriter().writeDebug(postMessage);
      } else if (firstRecord && !StringUtils.isNullOrEmpty(response.result)) {
        getWriter().writeDebug("First record arrival cost time: " + String.valueOf(System.currentTimeMillis() - startTime) + " ms.");
        firstRecord = false;

        getWriter().writeIntermediateResult(response.result);
      }
    }
  }

  private void stopSession() throws OdpsException {
    try {
      ExecutionContext.getSessionInstance().stop();
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {
      // user can stop session successfully with any exception.
      // because the session will stop without sub-query for a certain expire-time.
      ExecutionContext.setSessionInstance(null);
      getContext().setInteractiveQuery(false);
    }
  }

  private void stopSession(String id) throws ODPSConsoleException, OdpsException {
    try {
      Odps odps = OdpsConnectionFactory.createOdps(getContext());
      Instance instance = odps.instances().get(id);
      instance.stop();
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {
      // user can stop session successfully with any exception.
      // because the session will stop without sub-query for a certain expire-time.
      ExecutionContext.setSessionInstance(null);
      getContext().setInteractiveQuery(false);
    }
  }

  private Gson gson = new Gson();

  public InteractiveQueryCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {
    if (getCommandText().matches(startRegex)) {
      Pattern startPattern = Pattern.compile(startRegex);
      Matcher matcher = startPattern.matcher(getCommandText());
      matcher.matches();
      String sessionName = matcher.group(1);
      startSession(sessionName);
    } else if (getCommandText().matches(useRegex)) {
      Pattern usePattern = Pattern.compile(useRegex);
      Matcher matcher = usePattern.matcher(getCommandText());
      matcher.matches();
      String id = matcher.group(1);
      useSession(id);
    } else if (getCommandText().matches(attachRegex)) {
      Pattern attachPattern = Pattern.compile(attachRegex);
      Matcher matcher = attachPattern.matcher(getCommandText());
      matcher.matches();
      String attachName = matcher.group(1);
      attachSession(attachName);
    } else if (getCommandText().matches(stopRegex)) {
      if (ExecutionContext.getSessionInstance() == null) {
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
        return;
      }
      stopSession();
    } else if (getCommandText().matches(stopWithIdRegex)) {
      Pattern stopPattern = Pattern.compile(stopWithIdRegex);
      Matcher matcher = stopPattern.matcher(getCommandText());
      matcher.matches();
      String id = matcher.group(1);
      stopSession(id);
    } else {
      submitSubQuery();
    }
  }

  public static InteractiveQueryCommand parse(String commandString, ExecutionContext sessionContext) {
    boolean matched = false;
    if (commandString.matches(startRegex) ||
        commandString.matches(useRegex) ||
        commandString.matches(attachRegex)) {
      matched = true;
    } else if (commandString.matches(stopRegex) ||
        commandString.matches(stopWithIdRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(false);
    }
    if (matched || sessionContext.isInteractiveQuery()) {
      commandString = commandString.trim();
      sessionContext.setInteractiveQuery(true);
      return new InteractiveQueryCommand(commandString, sessionContext);
    }
    return null;
  }
}

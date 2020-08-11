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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.sqa.SQLExecutor;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.Sessions;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.state.InstanceProgressReporter;
import com.aliyun.openservices.odps.console.output.state.InstanceStateContext;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import com.aliyun.openservices.odps.console.utils.SessionUtils;
import com.aliyun.openservices.odps.console.utils.SignalUtil;

import sun.misc.SignalHandler;

/**
 * Created by dejun.xiedj on 2017/10/27.
 */
public class InteractiveQueryCommand extends MultiClusterCommandBase {

  private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static String startRegex = "start\\s+session(\\s+(public\\.){0,1}\\w+)?";
  private static String useRegex = "use\\s+session\\s+(\\w+)";
  private static String attachRegex = "attach\\s+session\\s+((public\\.){0,1}\\w+)";
  private static String stopRegex = "stop\\s+session";
  private static String detachRegex = "detach\\s+session";
  private static String stopWithIdRegex = "stop\\s+session\\s+(\\w+)";
  private static String listRegex = "list\\s+session";
  private static String setInformationRegex = "session\\s+set\\s+(\\w+)\\s+(\\S+)";
  private static String getInformationRegex = "session\\s+get\\s+(\\w+)";
  private static String showVarsRegex = "show\\s+variables";

  class ReporterThread extends Thread {

    private final byte[] lock = new byte[0];
    private boolean finished = false;
    private InstanceStateContext context = null;
    private Odps odps;
    private ExecutionContext executionContext;
    private AtomicBoolean disableOutput = new AtomicBoolean(false);
    private InstanceProgressReporter reporter = null;
    private SQLExecutor sqlExecutor = null;

    public ReporterThread(Odps currentOdps, SQLExecutor sqlExecutor, ExecutionContext context) {
      this.odps = currentOdps;
      this.sqlExecutor = sqlExecutor;
      this.executionContext = context;
    }

    public InstanceProgressReporter getReporter() {
      return reporter;
    }

    public void finish() {
      synchronized (lock) {
        finished = true;
      }
    }

    private boolean isFinished() {
      synchronized (lock) {
        return finished;
      }
    }

    InstanceStateContext getStateContext() {
      return this.context;
    }

    private void resetReporter() {
      context = constructStateContext();
      reporter = new InstanceProgressReporter(context);
    }

    private void checkQueryStatus() {
      List<String> logs = sqlExecutor.getExecutionLog();
      if (!logs.isEmpty()) {
        // rerun or fallback, recreate reporter
        for (String log : logs) {
          executionContext.getOutputWriter().writeDebug(log);
        }
        resetReporter();
      }
      // init reporter
      if (context == null) {
        resetReporter();
      }
    }

    private void registerSignalHandler() {
      Thread thread = currentThread();
      SignalHandler instanceRunningIntSignalHandler =
          SignalUtil.getDefaultIntSignalHandler(thread);
      SignalUtil.registerSignalHandler("INT", instanceRunningIntSignalHandler);
    }

    @Override
    public void run() {

      registerSignalHandler();
      boolean shouldCancel = false;
      try {
        while (!isFinished()) {
          checkQueryStatus();
          try {
            if (shouldCancel) {
              getWriter().writeError("Cancelling query");
              sqlExecutor.cancel();
              shouldCancel = false;
            }
            List<Instance.StageProgress> stages = sqlExecutor.getProgress();

            context.setTaskProgress(stages);
            if (disableOutput.get() == false) {
              reporter.printProgress(false);
            }
          } catch (Exception e) {
            context.getExecutionContext().getOutputWriter()
                    .writeDebug(String.format("%s: %s, %s", "Instance progress reporter error",
                            e.getMessage(), StringUtils.stringifyException(e)));
          }
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            shouldCancel = true;
          }
        }
      } finally {
        if (disableOutput.get() == false) {
          registerSignalHandler();
        }
      }
      if (disableOutput.get() == false) {
        registerSignalHandler();
      }
    }

    public InstanceStateContext constructStateContext() {
      InstanceStateContext isContext = null;
      try {
        Instance instance = sqlExecutor.getInstance();
        isContext = new InstanceStateContext(odps, instance, executionContext);
        isContext.setInstanceStartTime(System.currentTimeMillis());
      } catch (OdpsException e) {
        executionContext.getOutputWriter().writeDebug(String.format("%s: %s, %s", "Instance progress reporter error",
                e.getMessage(), StringUtils.stringifyException(e)));
      }
      return isContext;
    }

    public void disableOutput() {
      disableOutput.compareAndSet(false, true);
    }
  }

  private String getProgressFormatString(Session.SessionProgress progress) {
    return String.format("%s Session starting: %d/%d [%d%%]", DATE_FORMAT.format(new Date()),
        progress.launchedWorkerCount, progress.totalWorkerCount,
        progress.launchedPercentage);
  }

  private void finishReporter(ReporterThread reporterThread) {
    try {
      interruptReporter(reporterThread);
      // query正常结束强行刷新进度至 100% 状态
      InstanceStateContext instanceStateContext = reporterThread.getStateContext();
      if (instanceStateContext != null) {
        InstanceProgressReporter reporter = reporterThread.getReporter();
        if (reporter != null) {
          reporter.printProgress(true);
        }
      }
    } catch (Exception ignore) {
    }
  }

  private void interruptReporter(ReporterThread reporterThread) {
    reporterThread.finish();
    reporterThread.disableOutput();
    reporterThread.interrupt();
  }

  private void printRecords(ResultSet resultSet)
      throws IOException, ODPSConsoleException {
    RecordPrinter recordPrinter = RecordPrinter.createReporter(resultSet.getTableSchema(), getContext());
    // header
    recordPrinter.printFrame();
    recordPrinter.printTitle();
    recordPrinter.printFrame();
    while (resultSet.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();
      Record record = resultSet.next();
      if (record == null) {
        break;
      }
      recordPrinter.printRecord(record);
    }
    // footer
    recordPrinter.printFrame();
  }

  private void getQueryResult(long startTime) throws ODPSConsoleException {
    SQLExecutor executor = ExecutionContext.getExecutor();
    ReporterThread reporterThread =
        new ReporterThread(getCurrentOdps(), executor,
            getContext());
    reporterThread.setDaemon(true);
    reporterThread.start();
    try {
      ResultSet resultSet = executor.getResultSet();
      finishReporter(reporterThread);
      if (resultSet.getTableSchema() != null && resultSet.getTableSchema().getColumns().size() != 0) {
        printRecords(resultSet);
      } else {
        // non-select query
      }
      String postMessage = "Session sub-query" + " cost time: "
          + String.valueOf(System.currentTimeMillis() - startTime) + " ms.";
      getWriter().writeDebug(postMessage);
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.toString(), e);
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    } finally {
      if (reporterThread.isAlive()) {
        interruptReporter(reporterThread);
      }
    }
  }
  /* ----------------------------- */
  /* Command handlers */
  /* public cmd */
  private void submitSubQuery() throws OdpsException, ODPSConsoleException {
    long startTime = System.currentTimeMillis();
    String commandString = getCommandText();
    if (!commandString.endsWith(";")) {
      commandString = commandString + ";";
    }
    SQLExecutor executor = ExecutionContext.getExecutor();
    if (executor == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }

    boolean isSelect = commandString.toUpperCase().matches("^SELECT[\\s\\S]*");

    HashMap<String, String> settings = new HashMap();
    if (!SetCommand.setMap.isEmpty()) {
      for (Map.Entry<String, String> property : SetCommand.setMap.entrySet()) {
        settings.put(property.getKey(), property.getValue());
      }
    }
    // force csv, will parse result from csv if tunnel disabled
    settings.put("odps.sql.select.output.format", "csv");

    executor.run(commandString, settings);

    getQueryResult(startTime);

    // 如果返回是空的,且不是 select 语句则打出OK
    if (!isSelect) {
      getWriter().writeError("OK");
    }
  }

  private void detachSession() throws OdpsException {
    try {
      ExecutionContext.getExecutor().close();
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {
      SessionUtils.clearSessionContext(getContext());
    }
  }

  private void attachSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getExecutor() != null
        && ExecutionContext.getExecutor().getInstance() != null) {
      getWriter().writeDebug("You are already in a session. Session id: " +
          ExecutionContext.getExecutor().getInstance().getId() +
          ", will reattach to the new session: " + sessionName);
      getContext().setInteractiveQuery(false);
      ExecutionContext.setInstanceRunner(null);
    }

    try {
      Session session = Session.attach(
          getCurrentOdps(),
          sessionName,
          SetCommand.setMap,
          0L,
          getContext().getRunningCluster(),
          ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME
      );

      String startSessionMessage = session.getStartSessionMessage();
      if (startSessionMessage != null && startSessionMessage.length() > 0) {
        getWriter().writeError(startSessionMessage);
      }

      SessionUtils.resetSQLExecutor(sessionName, session.getInstance(), getContext(), getCurrentOdps(), true);
    } catch (OdpsException ex) {
      getContext().setInteractiveQuery(false);
      throw ex;
    } catch (ODPSConsoleException ex) {
      getContext().setInteractiveQuery(false);
      throw ex;
    }
  }

  /* internal cmd */
  /* BEGIN: opensource removal */
  private void startSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getExecutor() != null
        && ExecutionContext.getExecutor().getInstance() != null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You are already in a session. Session id: " +
          ExecutionContext.getExecutor().getInstance().getId());
      return;
    }

    Session session = null;
    try {
      session = Session.create(
          getCurrentOdps(),
          sessionName,
          getCurrentProject(),
          SetCommand.setMap,
          null, getContext().getPriority(), getContext().getRunningCluster()
      );

      while (!session.isStarted()) {
        Session.SessionProgress progress = session.getStartProgress();

        if (progress != null) {
          getWriter()
              .writeError(getProgressFormatString(progress));
        }

        try {
          Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
          throw new UserInterruptException("interrupted while thread sleep");
          // Just return if the thread is interrupted
        }
      }
    } catch (Exception e) {
      getContext().setInteractiveQuery(false);
      throw e;
    }

    getWriter().writeError(DATE_FORMAT.format(new Date()) + " Session started.");

    SessionUtils.resetSQLExecutor(sessionName, session.getInstance(), getContext(),
        getCurrentOdps(), false/*can not reattach owner session*/);
    String startSessionMessage = session.getStartSessionMessage();
    if (startSessionMessage != null && startSessionMessage.length() > 0) {
      getWriter().writeError(startSessionMessage);
    }
  }

  private void stopSession() throws OdpsException {
    try {
      ExecutionContext.getExecutor().close();
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {
      // user can stop session successfully with any exception.
      // because the session will stop without sub-query for a certain expire-time.
      SessionUtils.clearSessionContext(getContext());
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
      SessionUtils.resetSessionContext(null, null, getContext());
    }
  }

  private void useSession(String id) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getExecutor() != null
        && ExecutionContext.getExecutor().getInstance() != null) {
      getWriter().writeDebug("You are already in a session. Session id: " +
          ExecutionContext.getExecutor().getInstance().getId() +
          ", will switch to the this new session: " + id);
      getContext().setInteractiveQuery(false);
      ExecutionContext.setInstanceRunner(null);
    }

    try {
      Odps odps = getCurrentOdps();
      Instance instance = odps.instances().get(id);
      if (!instance.getStatus().equals(Instance.Status.RUNNING)) {
        throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Session " + id + " is not running, status:" + instance.getStatus().toString());
      } else {
        SessionUtils.recoverSQLExecutor(instance, getContext(), odps, false/* do not reattach in use mode*/);
      }
    } catch (NoSuchObjectException ex) {
      getContext().setInteractiveQuery(false);
      throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Session " + id + " is not exist. Try to check the session ID.");
    } catch (Exception ex) {
      getContext().setInteractiveQuery(false);
      throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Use session " + id + " failed. " + ex.toString(), ex);
    }
  }

  private void listSession() {
    try {
      Odps odps = OdpsConnectionFactory.createOdps(getContext());
      Sessions sessions = new Sessions(odps);
      Iterator<Session.SessionItem> iter = sessions.iterator();

      while (iter.hasNext()) {
        Session.SessionItem item = iter.next();
        getWriter().writeResult("Id:" + item.sessionId + ", Name:" + item.aliasName + ", version:" + item.version + ". owner:" + item.owner);
      }
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {

    }
  }

  private void showVariables() throws OdpsException, ODPSConsoleException {
    Session session = getCurrentSession();
    if (session == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }
    try {
      List<String> vars = session.showVariables(SetCommand.setMap);
      for (String var : vars) {
        getWriter().writeResult(var);
      }
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {

    }
  }

  private void getInformation(String key) throws OdpsException, ODPSConsoleException {
    Session session = getCurrentSession();
    if (session == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }
    try {
      String result = session.getInformation(key);
      getWriter().writeResult(result);
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {

    }
  }

  private void setInformation(String key, String value) throws OdpsException, ODPSConsoleException {
    Session session = getCurrentSession();
    if (session == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }
    String result = null;
    try {
      result = session.setInformation(key, value);
      if (StringUtils.isNullOrEmpty(result)) {
        getWriter().writeResult("RequestFailed");
      } else {
        getWriter().writeResult(result);
      }
    } catch (Exception ex) {
      getWriter().writeError(result);
      getWriter().writeError(ex.toString());
    } finally {

    }
  }
  /* END: opensource removal */

  private Session getCurrentSession() throws OdpsException, ODPSConsoleException {
    SQLExecutor executor = ExecutionContext.getExecutor();
    if (executor != null && executor.getInstance() != null) {
      return new Session(getCurrentOdps(), executor.getInstance());
    }
    return null;
  }

  public InteractiveQueryCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    /* BEGIN: opensource removal */
    if (getCommandText().matches(startRegex)) {
      Pattern startPattern = Pattern.compile(startRegex);
      Matcher matcher = startPattern.matcher(getCommandText());
      matcher.matches();
      String sessionName = matcher.group(1);
      startSession(sessionName);
      return;
    }
    if (getCommandText().matches(useRegex)) {
      Pattern usePattern = Pattern.compile(useRegex);
      Matcher matcher = usePattern.matcher(getCommandText());
      matcher.matches();
      String id = matcher.group(1);
      useSession(id);
      return;
    }
    if (getCommandText().matches(stopRegex)) {
      if (ExecutionContext.getExecutor() == null) {
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + ", You need to use or start a session in advance.");
        return;
      }
      stopSession();
      return;
    }
    if (getCommandText().matches(stopWithIdRegex)) {
      Pattern stopPattern = Pattern.compile(stopWithIdRegex);
      Matcher matcher = stopPattern.matcher(getCommandText());
      matcher.matches();
      String id = matcher.group(1);
      stopSession(id);
      return;
    }
    if (getCommandText().matches(getInformationRegex)) {
      Pattern getPattern = Pattern.compile(getInformationRegex);
      Matcher matcher = getPattern.matcher(getCommandText());
      matcher.matches();
      String key = matcher.group(1);
      getInformation(key);
      return;
    }
    if (getCommandText().matches(setInformationRegex)) {
      Pattern setPattern = Pattern.compile(setInformationRegex);
      Matcher matcher = setPattern.matcher(getCommandText());
      matcher.matches();
      String key = matcher.group(1);
      String value = matcher.group(2);
      setInformation(key, value);
      return;
    }
    if (getCommandText().matches(listRegex)) {
      listSession();
      return;
    }
    if (getCommandText().matches(showVarsRegex)) {
      if (ExecutionContext.getExecutor() == null) {
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + ", You need to use or start a session in advance.");
        return;
      }
      showVariables();
      return;
    }
    /* END: opensource removal */
    if (getCommandText().matches(attachRegex)) {
      Pattern attachPattern = Pattern.compile(attachRegex);
      Matcher matcher = attachPattern.matcher(getCommandText());
      matcher.matches();
      String attachName = matcher.group(1);
      attachSession(attachName);
      return;
    }

    if (getCommandText().matches(detachRegex)) {
      if (ExecutionContext.getExecutor() == null) {
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + ", You need to attach a session in advance.");
        return;
      }
      detachSession();
      return;
    }
    submitSubQuery();
  }

  public static InteractiveQueryCommand parse(String commandString, ExecutionContext sessionContext) {
    boolean matched = false;
    /* BEGIN: opensource removal */
    if (commandString.matches(startRegex) ||
        commandString.matches(useRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(true);
    } else if (commandString.matches(stopRegex) ||
        commandString.matches(stopWithIdRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(false);
    } else if (commandString.matches(listRegex) ||
        commandString.matches(setInformationRegex) ||
        commandString.matches(getInformationRegex)) {
      matched = true;
    }
    /* END: opensource removal */

    if (commandString.matches(attachRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(true);
    } else if (commandString.matches(detachRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(false);
    }

    if (matched || sessionContext.isInteractiveQuery()) {
      commandString = commandString.trim();
      return new InteractiveQueryCommand(commandString, sessionContext);
    }
    return null;
  }
}

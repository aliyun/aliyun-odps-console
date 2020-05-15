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
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.openservices.odps.console.utils.*;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
import com.aliyun.odps.Sessions;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SessionQueryResult;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.state.InstanceProgressReporter;
import com.aliyun.openservices.odps.console.output.state.InstanceStateContext;

import sun.misc.SignalHandler;

/**
 * Created by dejun.xiedj on 2017/10/27.
 */
public class InteractiveQueryCommand extends MultiClusterCommandBase {

  private static int OBJECT_STATUS_RUNNING = 2;
  private static int OBJECT_STATUS_FAILED = 4;
  private static int OBJECT_STATUS_TERMINATED = 5;
  private static int OBJECT_STATUS_CANCELLED = 6;

  private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static String startRegex = "start\\s+session(\\s+(public\\.){0,1}\\w+)?";
  private static String useRegex = "use\\s+session\\s+(\\w+)";
  private static String attachRegex = "attach\\s+session\\s+((public\\.){0,1}\\w+)";
  private static String stopRegex = "stop\\s+session";
  private static String detachRegex = "detach\\s+session";
  private static String stopWithIdRegex = "stop\\s+session\\s+(\\w+)";
  private static String listRegex = "list\\s+session";
  private static String setInformationRegex = "session\\s+set\\s+(\\w+)\\s+(\\w+)";
  private static String getInformationRegex = "session\\s+get\\s+(\\w+)";
  private int currentQueryId = -1;

  private static String odpsTaskMajorVersion = "odps.task.major.version";
  private static String sessionRerunFlag = "ODPS-185";

  class ReporterThread extends Thread {

    private final byte[] lock = new byte[0];
    private boolean finished = false;
    private InstanceStateContext context = null;
    private SessionQueryResult query;
    private Odps odps;
    private Instance instance;
    private ExecutionContext executionContext;
    private AtomicBoolean disableOutput = new AtomicBoolean(false);
    private InstanceProgressReporter reporter = null;

    public ReporterThread(Odps currentOdps, Instance instance, ExecutionContext context, SessionQueryResult subqueryResult) {
      this.odps = currentOdps;
      this.instance = instance;
      this.executionContext = context;
      this.query = subqueryResult;
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

    @Override
    public void run() {
      context = constructStateContext();
      context.setInstanceStartTime(System.currentTimeMillis());
      reporter = new InstanceProgressReporter(context);

      Thread thread = currentThread();
      SignalHandler instanceRunningIntSignalHandler =
              SignalUtil.getDefaultIntSignalHandler(thread);
      SignalUtil.registerSignalHandler("INT", instanceRunningIntSignalHandler);
      boolean shouldCancel = false;
      try {
        while (!isFinished()) {
          try {
            if (shouldCancel) {
              getWriter().writeError("Cancelling query");
              ExecutionContext.getSessionInstance().cancelQuery(query.getSubQueryInfo().queryId);
              shouldCancel = false;
            }
            List<Instance.StageProgress> stages =
                ExecutionContext.getSessionInstance().getInstance().getTaskProgress(ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME);

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
          SignalHandler defaultIntSignalHandler =
              SignalUtil.getDefaultIntSignalHandler(Thread.currentThread());
          SignalUtil.registerSignalHandler("INT", defaultIntSignalHandler);
        }
      }
      if (disableOutput.get() == false) {
        SignalHandler defaultIntSignalHandler =
            SignalUtil.getDefaultIntSignalHandler(Thread.currentThread());
        SignalUtil.registerSignalHandler("INT", defaultIntSignalHandler);
      }
    }

    public InstanceStateContext constructStateContext() {
      InstanceStateContext isContext = null;
      try {
        isContext = new InstanceStateContext(odps, instance, executionContext);
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

  private void initSqlTimeZone() {
    // if timezone not setted, this printer will call odps service to get project setting
    // which will cost at least 800ms
    try {
      String timezode = getCurrentOdps().projects().get(getContext().getProjectName()).getProperty("odps.sql.timezone");
      if (StringUtils.isNullOrEmpty(timezode)) {
        timezode = TimeZone.getDefault().getID();
        getWriter().writeDebug("Use default timezone:" + timezode);
      } else {
        getWriter().writeDebug("Use project timezone:" + timezode);
      }
      getContext().setSqlTimezone(timezode);

    } catch (Exception e) {
      getWriter().writeError(e.getMessage());
    }
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

  private void printStreamingRecords(TunnelRecordReader reader, TableSchema schema, ReporterThread reporterThread, long startTime)
      throws IOException, ODPSConsoleException {
    boolean firstRecord = true;
    RecordPrinter recordPrinter = null;
    while (true) {
      ODPSConsoleUtils.checkThreadInterrupted();
      // long polling request
      Record record = reader.read();
      if (record == null) {
        break;
      }
      if (firstRecord) {
        // set progress to 100%
        finishReporter(reporterThread);
        getWriter().writeDebug("First record arrival cost time: " + String.valueOf(System.currentTimeMillis() - startTime) + " ms.");
        recordPrinter = new RecordPrinter(schema, getCurrentOdps(), getContext());
        recordPrinter.printFrame();
        recordPrinter.printTitle();
        recordPrinter.printFrame();
        firstRecord = false;
      }
      recordPrinter.printRecord(record);
    }
    if (recordPrinter != null) {
      recordPrinter.printFrame();
    }
  }

  private void handleSessionFail(String commandString, String result)
      throws ODPSConsoleException, OdpsException {
    String version = null;
    ExecutionContext context = getContext();
    try {
      if (result.indexOf(sessionRerunFlag) != -1 && getContext().isSessionAutoRerun()) {
        getWriter().writeError("Warnings: rerun in offline mode, fallback reason: " + result);
        context.setInteractiveQuery(false);
        // always rerun in default version
        if (SetCommand.setMap.containsKey(odpsTaskMajorVersion)) {
          version = SetCommand.setMap.get(odpsTaskMajorVersion);
          SetCommand.setMap.put(odpsTaskMajorVersion, "default");
        }
        AbstractCommand q = QueryCommand.parse(commandString, getContext());
        q.execute();
      } else {
        throw new ODPSConsoleException(result);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      // set back version
      context.setInteractiveQuery(true);
      if (version != null) {
        SetCommand.setMap.put(odpsTaskMajorVersion, version);
      }
    }
  }

  private void queryResult(
      long startTime,
      SessionQueryResult sessionQueryResult,
      String commandString,
      ReporterThread reporterThread)
          throws OdpsException, ODPSConsoleException {
    boolean firstRecord = true;
    try {
      Iterator<Session.SubQueryResponse> responseIterator = sessionQueryResult.getResultIterator();
      while (responseIterator.hasNext()) {
        Session.SubQueryResponse response = responseIterator.next();
        if (!StringUtils.isNullOrEmpty(response.warnings)) {
          getWriter().writeError("Warnings: " + response.warnings);
        }

        if (response.status == OBJECT_STATUS_FAILED) {
          interruptReporter(reporterThread);
          handleSessionFail(commandString, response.result);
        } else if (response.status != OBJECT_STATUS_RUNNING) {
          if (firstRecord) {
            // 如果在第一批数据来临时已经刷过进度条则不再刷新,否则需要强刷到100%
            finishReporter(reporterThread);
          }
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
          // 第一条记录来临,若进度打印没有完成，则强行刷新进度至 100% 状态
          finishReporter(reporterThread);

          getWriter().writeIntermediateResult(response.result);
        }
      }
    } catch(Exception e) {
      if (reporterThread.isAlive()) {
        interruptReporter(reporterThread);
      }
      throw new ODPSConsoleException(e.getMessage());
    }
  }


  private void queryResultByInstanceTunnel(
      long startTime,
      String commandString,
      ReporterThread reporterThread)
          throws OdpsException, ODPSConsoleException {

    try {
      Odps odps = getCurrentOdps();
      InstanceTunnel tunnel = new InstanceTunnel(odps);

      InstanceTunnel.DownloadSession downloadSession = tunnel.createDirectDownloadSession(
          getCurrentProject(),
          ExecutionContext.getSessionInstance().getInstance().getId(),
          ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME, currentQueryId);
      TunnelRecordReader reader = downloadSession.openRecordReader(0, 1);
      printStreamingRecords(reader, downloadSession.getSchema(), reporterThread, startTime);
      reader.close();
      String postMessage = "Session sub-query:" + currentQueryId + " cost time: " + String.valueOf(System.currentTimeMillis() - startTime) + " ms.";
      getWriter().writeDebug(postMessage);
    } catch (Exception e) {
      if (reporterThread.isAlive()) {
        interruptReporter(reporterThread);
      }
      handleSessionFail(commandString, e.getMessage());
    }
  }

  private void clearSessionContext() {
    ExecutionContext.setSessionInstance(null);
    getContext().setInteractiveQuery(false);
    if (getContext().getAutoSessionMode()) {
      LocalCacheUtils.clearCache();
    }
  }

  private SessionQueryResult submitQuery(String commandString, HashMap<String, String> settings, int retryTime)
    throws OdpsException,ODPSConsoleException {
    if (retryTime >= 2) {
      throw new OdpsException("Submit query failed, retry time:" + retryTime);
    }
    SessionQueryResult subQueryResult = ExecutionContext.getSessionInstance().run(commandString, settings);
    Session.SubQueryInfo subQueryInfo = subQueryResult.getSubQueryInfo();
    if (subQueryInfo != null) {
      if (subQueryInfo.status.equals(Session.SubQueryInfo.kNotFoundCode)) {
        getWriter().writeDebug("Current session has stopped, error:" + subQueryInfo.result);
        clearSessionContext();
        if (getContext().getAutoSessionMode()) {
          SessionUtils.autoAttachSession(getContext(), getCurrentOdps());
          //retry once
          return submitQuery(commandString, settings, retryTime + 1);
        } else {
          throw new OdpsException("Submit query failed:" + subQueryInfo.result);
        }
      } else if (subQueryInfo.status.equals(Session.SubQueryInfo.kFailedCode)) {
        throw new OdpsException("Submit query failed:" + subQueryInfo.result);
      } else {
        currentQueryId = subQueryInfo.queryId;
        getWriter().writeDebug("Current queryId:" + currentQueryId);
      }
    } else {
      // will get latest query, never reach here by design
      getWriter().writeDebug("Parse submit response failed, empty response, " +
          "will get result from currently running query");
      currentQueryId = -1;
    }
    return subQueryResult;
  }

  /* ----------------------------- */
  /* Command handlers */
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

    boolean isSelect = commandString.toUpperCase().matches("^SELECT[\\s\\S]*");

    HashMap<String, String> settings = new HashMap();
    if (!SetCommand.setMap.isEmpty()) {
      for (Map.Entry<String, String> property : SetCommand.setMap.entrySet()) {
        settings.put(property.getKey(), property.getValue());
      }
    }
    if (!getContext().isMachineReadable()) {
      settings.put("odps.sql.select.output.format", "HumanReadable");
    }

    SessionQueryResult subQueryResult = submitQuery(commandString, settings, 0);

    boolean useInstanceTunnel = getContext().isUseInstanceTunnel();
    ReporterThread reporterThread =
        new ReporterThread(getCurrentOdps(), ExecutionContext.getSessionInstance().getInstance(),
            getContext(),subQueryResult);
    reporterThread.setDaemon(true);
    reporterThread.start();

    if (useInstanceTunnel) {
      queryResultByInstanceTunnel(startTime, commandString, reporterThread);
    } else {
      queryResult(startTime, subQueryResult, commandString, reporterThread);
    }

    // 如果返回是空的,且不是 select 语句则打出OK
    if (!isSelect) {
      getWriter().writeError("OK");
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
      clearSessionContext();
    }
  }

  private void detachSession() throws OdpsException {
    try {
      ExecutionContext.getSessionInstance().stop();
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {
      clearSessionContext();
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

  private void startSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getSessionInstance() != null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You are already in a session. Session name: " +
          ExecutionContext.getSessionInstance().getSessionName());
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

      session.printLogView();

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

    ExecutionContext.setSessionInstance(session);
    getContext().setInteractiveQuery(true);
    String startSessionMessage = session.getStartSessionMessage();
    if (startSessionMessage != null && startSessionMessage.length() > 0) {
      getWriter().writeError(startSessionMessage);
    }
    initSqlTimeZone();
  }

  private void attachSession(String sessionName) throws OdpsException, ODPSConsoleException {
    if (ExecutionContext.getSessionInstance() != null) {
      getWriter().writeDebug("You are already in a session. Session name: " +
          ExecutionContext.getSessionInstance().getSessionName() +
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
      ExecutionContext.setSessionInstance(session);
      getContext().setInteractiveQuery(true);
      session.printLogView();
      String startSessionMessage = session.getStartSessionMessage();
      if (startSessionMessage != null && startSessionMessage.length() > 0) {
        getWriter().writeError(startSessionMessage);
      }
      initSqlTimeZone();
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
      getWriter().writeDebug("You are already in a session. Session name: " +
          ExecutionContext.getSessionInstance().getSessionName() +
          ", will reattach to the new session: " + id);
      getContext().setInteractiveQuery(false);
      ExecutionContext.setInstanceRunner(null);
    }

    Odps odps = getCurrentOdps();
    Instance instance = odps.instances().get(id);
    try {
      if (instance.getTaskStatus().containsKey(ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME)
          && instance.getTaskStatus().get(ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME).getStatus() != Instance.TaskStatus.Status.RUNNING) {
        throw new OdpsException(ODPSConsoleConstants.FAILED_MESSAGE + "Session " + id + " is not running.");
      } else {
        Session session = new Session(odps, instance);
        ExecutionContext.setSessionInstance(session);
        getContext().setInteractiveQuery(true);
        initSqlTimeZone();
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

  private void getInformation(String key) {
    if (ExecutionContext.getSessionInstance() == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }
    try {
      String result = ExecutionContext.getSessionInstance().getInformation(key);
      getWriter().writeResult(result);
    } catch (Exception ex) {
      getWriter().writeDebug(ex.toString());
    } finally {

    }
  }

  private void setInformation(String key, String value) {
    if (ExecutionContext.getSessionInstance() == null) {
      getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + "You need to start, use or attach session in advance.");
      return;
    }
    String result = null;
    try {
      result = ExecutionContext.getSessionInstance().setInformation(key, value);
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

  public InteractiveQueryCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
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
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + ", You need to use or start a session in advance.");
        return;
      }
      stopSession();
    } else if (getCommandText().matches(detachRegex)) {
      if (ExecutionContext.getSessionInstance() == null) {
        getWriter().writeError(ODPSConsoleConstants.FAILED_MESSAGE + ", You need to attach a session in advance.");
        return;
      }
      detachSession();
    } else if (getCommandText().matches(stopWithIdRegex)) {
      Pattern stopPattern = Pattern.compile(stopWithIdRegex);
      Matcher matcher = stopPattern.matcher(getCommandText());
      matcher.matches();
      String id = matcher.group(1);
      stopSession(id);
    } else if (getCommandText().matches(getInformationRegex)) {
      Pattern getPattern = Pattern.compile(getInformationRegex);
      Matcher matcher = getPattern.matcher(getCommandText());
      matcher.matches();
      String key = matcher.group(1);
      getInformation(key);
    } else if (getCommandText().matches(setInformationRegex)) {
      Pattern setPattern = Pattern.compile(setInformationRegex);
      Matcher matcher = setPattern.matcher(getCommandText());
      matcher.matches();
      String key = matcher.group(1);
      String value = matcher.group(2);
      setInformation(key, value);
    } else if (getCommandText().matches(listRegex)) {
      listSession();
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
      sessionContext.setInteractiveQuery(true);
    } else if (commandString.matches(stopRegex) ||
            commandString.matches(stopWithIdRegex) ||
            commandString.matches(detachRegex)) {
      matched = true;
      sessionContext.setInteractiveQuery(false);
    } else if (commandString.matches(listRegex) ||
            commandString.matches(setInformationRegex) ||
            commandString.matches(getInformationRegex)) {
      matched = true;
    }

    if (matched || sessionContext.isInteractiveQuery()) {
      commandString = commandString.trim();
      return new InteractiveQueryCommand(commandString, sessionContext);
    }
    return null;
  }
}

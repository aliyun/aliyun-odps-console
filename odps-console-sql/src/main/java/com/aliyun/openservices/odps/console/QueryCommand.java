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
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.task.SqlPlanTask;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import com.google.gson.*;
import org.jline.reader.UserInterruptException;

/**
 * 提交匿名job，执行query
 *
 * @author shuman.gansm
 */
public class QueryCommand extends MultiClusterCommandBase {

  private String taskName = "";

  private boolean isSelectCommand = false;

  private DateFormat dateFormat = null;
  private DateFormat timestampFormat = null;

  // Gson customized serializer
  private JsonSerializer<Date> dateTimeSerializer;
  private JsonSerializer<Timestamp> timestampSerializer;
  private JsonSerializer<SimpleStruct> structSerializer;
  private Gson gson;

  private Double getSQLInputSizeInGB() {
    try {
      String queryResult = runSQLCostTask();

      JsonObject node = new JsonParser().parse(queryResult).getAsJsonObject();

      if (!node.has("Cost") || !node.get("Cost").getAsJsonObject().has("SQL") ||
              !node.get("Cost").getAsJsonObject().get("SQL").getAsJsonObject().has("Input")) {
        return null;
      }

      Double input = node.get("Cost").getAsJsonObject().get("SQL").getAsJsonObject().get("Input").getAsDouble();
      if (input != null) {
        return input / 1024 / 1024 / 1024;
      }

    } catch (Exception e) {
      // ignore
    }

    return null;
  }

  private String runSQLCostTask() throws OdpsException, ODPSConsoleException {
    String taskName = "console_cost_query_task_" + Calendar.getInstance().getTimeInMillis();
    SQLCostTask task = new SQLCostTask();
    task.setName(taskName);
    task.setQuery(getCommandText());

    HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

    for (Entry<String, String> property : taskConfig.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }

    Instance instance = getCurrentOdps().instances().create(task);
    instance.waitForSuccess();

    String result = instance.getTaskResults().get(task.getName());
    return result;
  }

  private String getConfirmMessage() {
    // display to MB
    return String.format(
        "WARNING! Input data > %.3fGB, might be slow or expensive, proceed anyway (yes/no)?",
        getContext().getConfirmDataSize());
  }

  private boolean isConfirm() throws ODPSConsoleException {
    String prompt = getConfirmMessage();
    String inputStr = "";

    while (true) {
      inputStr = ODPSConsoleUtils.getOdpsConsoleReader().readLine(prompt);

      if (inputStr == null) {
        return false;
      }

      if (inputStr.trim().toUpperCase().equals("N") || inputStr.trim().toUpperCase().equals("NO")) {
        return false;
      } else if (inputStr.trim().toUpperCase().equals("Y")
                 || inputStr.trim().toUpperCase().equals("YES")) {
        return true;
      }
    }
  }

  private boolean confirmSQLInput() {
    Double threshold = getContext().getConfirmDataSize();
    if (threshold != null) {
      try {
        Double input = getSQLInputSizeInGB();

        if (input != null && input > threshold) {
          return isConfirm();
        }

      } catch (ODPSConsoleException e) {
        //ignore
      }
    }
    return true;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    if (isSelectCommand && getContext().isAsyncMode()) {
      getWriter().writeError("[async mode]: can't support select command.");
      return;
    }

    ExecutionContext context = getContext();

    if (context.isInteractiveMode()) {
      //check input confirm, if no return directly.
      if (!confirmSQLInput()) {
        return;
      }
    }

    boolean isDryRun = getContext().isDryRun();

    DefaultOutputWriter writer = context.getOutputWriter();

    // try retry
    int retryTime = isSelectCommand ? 1 : context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (retryTime > 0) {

      Task task = null;
      try {

        if (isDryRun) {
          taskName = "console_sqlplan_task_" + Calendar.getInstance().getTimeInMillis();
          task = new SqlPlanTask(taskName, getCommandText());
        } else {
          taskName = "console_query_task_" + Calendar.getInstance().getTimeInMillis();
          task = new SQLTask();
          task.setName(taskName);
          ((SQLTask) task).setQuery(getCommandText());
        }

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
        if (!getContext().isMachineReadable()) {
          Map<String, String> settings = new HashMap<String, String>();
          settings.put("odps.sql.select.output.format", "HumanReadable");
          addSetting(taskConfig, settings);
        }

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task);

        // success
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {

        // 如果是insert动态分区的sql，且判断是否能够重试
        if (task instanceof SQLTask && QueryUtil.isOperatorDisabled(((SQLTask) task).getQuery())) {

          String errorMessage = e.getMessage();

          // 两种情况不能够重试，
          // 第一明确是ODPS-0110999，
          // 第二种不包含ODPS-的未知异常，如excutor crash掉了，未知的网络问题
          if (errorMessage.indexOf("ODPS-0110999") != -1) {
            // 如果是异常是ODPS-0110999不允许重试
            throw new ODPSConsoleException(e.getMessage());
          } else if (errorMessage.indexOf("ODPS-") == -1) {
            // 如果不错误包含ODPS-语句，这是未知的异常
            throw new ODPSConsoleException("ODPS-0110999:" + e.getMessage());
          }

        }

        retryTime--;
        if (retryTime == 0) {
          throw new ODPSConsoleException(e.getMessage(), e);
        }

        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }

    // 如果返回是空的,且不是 select 语句则打出OK
    if (!isSelectCommand) {
      writer.writeError("OK");
    }
  }

  @Override
  protected void reportResult(InstanceRunner runner) throws OdpsException, ODPSConsoleException {
    if (isSelectCommand && getContext().isUseInstanceTunnel()) {
      reportByInstanceTunnel(runner);
    } else {
      super.reportResult(runner);
    }
  }

  private void reportByInstanceTunnel(InstanceRunner runner) throws OdpsException, ODPSConsoleException {
    Long sessionMaxRow = getContext().getinstanceTunnelMaxRecord();
    if (sessionMaxRow != null) {
      Long readMaxRow = getReadMaxRow();
      if (readMaxRow != null && sessionMaxRow <= readMaxRow) {
        reportResultSet(runner, sessionMaxRow, true);
      } else {
        reportResultSet(runner, readMaxRow == null ? sessionMaxRow : readMaxRow, false);
      }
      return;
    }
    reportResultSet(runner, null, false);
  }

  private void reportResultSet(InstanceRunner runner, Long maxRow,
                               boolean isLimited) throws OdpsException, ODPSConsoleException {
    ResultSet records;
    String tunnelEndpoint = getContext().getTunnelEndpoint();

    try {
      records =
          SQLTask.getResultSet(runner.getInstance(), taskName, maxRow, isLimited,
                               StringUtils.isNullOrEmpty(tunnelEndpoint) ? null
                                                                         : new URI(tunnelEndpoint));
      flushResult(records, maxRow, isLimited, runner.getInstance());

    } catch (OdpsException e) {
      if (getContext().isDebug()) {
        String message =
            String.format("Invoke %s getResultSet error: %s", isLimited ? "limited" : "unlimited",
                          e.getMessage());
        getContext().getOutputWriter().writeDebug(message);
      }
      if (!isLimited) {
        reportResultSet(runner, maxRow, true);
      } else {
        super.reportResult(runner);
      }
    } catch (URISyntaxException e) {
      throw new ODPSConsoleException("Illegal tunnel endpoint: " + tunnelEndpoint + "please check the config.", e);
    } catch (Exception e) {
      throw new ODPSConsoleException("Failed to get query result by instance tunnel, " + e.getMessage(),  e);
    }
  }

  private void flushResult(ResultSet resultSet, Long maxRow, boolean isLimited, Instance instance) {
    try {
      flushResultSet(resultSet);

      if (maxRow != null) {
        getContext().getOutputWriter().writeError(String.format(
            "%d records (at most %d supported) fetched by instance tunnel.",
            resultSet.getRecordCount(), maxRow));
      } else if (isLimited) {
        getContext().getOutputWriter().writeError(String.format(
            "%d records fetched by instance tunnel.", resultSet.getRecordCount()));
      } else {
        getContext().getOutputWriter().writeError(String.format(
            "A total of %d records fetched by instance tunnel.", resultSet.getRecordCount()));
      }

    } catch (UserInterruptException e) {
      getContext().getOutputWriter().writeError("Print result interrupted.");
      getContext().getOutputWriter().writeError(
          "Use \'tunnel download instance://" + instance.getId()
          + " <path>;\' to download the result of this instance.");
      throw e;
    }
  }

  private Long getReadMaxRow() {
    try {
      Map<String, String> properties =
          getCurrentOdps().projects().get(getCurrentProject()).getProperties();

      return Long.parseLong(properties.get("READ_TABLE_MAX_ROW"));
    } catch (Exception ignore) {
      if (getContext().isDebug()) {
        getContext().getOutputWriter()
            .writeDebug("get READ_TABLE_MAX_ROW error: " + ignore.getMessage());
      }
    }
    return null;
  }

  private String formatRecord(Record record, Map<String, Integer> width) {
    StringBuilder sb = new StringBuilder();
    sb.append("| ");

    for (int i = 0; i < record.getColumnCount(); i++) {
      String res = formatField(record.get(i), record.getColumns()[i].getTypeInfo());

      sb.append(res);
      if (res.length() < width.get(record.getColumns()[i].getName())) {
        int extraLen = width.get(record.getColumns()[i].getName()) - res.length();
        while (extraLen-- > 0) {
          sb.append(" ");
        }
      }

      sb.append(" | ");
    }

    return sb.toString();
  }

  private JsonElement normalizeStruct(Object object) {
    LinkedHashMap<String, Object> values = new LinkedHashMap<String, Object>();
    Struct struct = (Struct) object;
    for (int i = 0; i < struct.getFieldCount(); i++) {
      values.put(struct.getFieldName(i), struct.getFieldValue(i));
    }

    return new Gson().toJsonTree(values);
  }

  private String formatStruct(Object object) {
    return gson.toJson(normalizeStruct(object));
  }

  private String formatField(Object object, TypeInfo typeInfo) {
    if (object == null) {
      return "NULL";
    }

    switch (typeInfo.getOdpsType()) {
      case DATETIME: {
        return dateFormat.format((Date) object);
      }
      case TIMESTAMP: {
        return timestampFormat.format((java.sql.Timestamp) object);
      }
      case ARRAY:
      case MAP: {
        return gson.toJson(object);
      }
      case STRUCT: {
        return formatStruct(object);
      }
      case STRING: {
        if (object instanceof byte []) {
          return new String((byte []) object);
        }

        return object.toString();
      }
      default: {
        return object.toString();
      }
    }
  }

  private void initJsonConfig() {
    dateTimeSerializer = new JsonSerializer<Date>() {
      @Override
      public JsonElement serialize(Date date, Type type, JsonSerializationContext jsonSerializationContext) {
        if (date == null) {
          return null;
        }
        return new JsonPrimitive(dateFormat.format(date));
      }
    };

    timestampSerializer = new JsonSerializer<Timestamp>() {
      @Override
      public JsonElement serialize(Timestamp timestamp, Type type, JsonSerializationContext jsonSerializationContext) {
        if (timestamp == null) {
          return null;
        }
        return new JsonPrimitive(timestampFormat.format(timestamp));
      }
    };

    structSerializer = new JsonSerializer<SimpleStruct>() {
      @Override
      public JsonElement serialize(SimpleStruct struct, Type type, JsonSerializationContext jsonSerializationContext) {
        if (struct == null) {
          return null;
        }
        return normalizeStruct(struct);
      }
    };

    gson = new GsonBuilder()
            .registerTypeAdapter(Date.class, dateTimeSerializer)
            .registerTypeAdapter(Timestamp.class, timestampSerializer)
            .registerTypeAdapter(SimpleStruct.class, structSerializer)
            .serializeNulls()
            .create();
  }

  private void initDateFormat() {
    dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

    try {
      String timezone = getContext().getSqlTimezone();

      if (timezone == null) {
        timezone =
            getCurrentOdps().projects().get(getCurrentProject()).getProperty("odps.sql.timezone");
      }

      if (timezone != null) {
        dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        timestampFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        getContext().getOutputWriter().writeDebug("SQL records formatted in timezone: " + timezone);
      }
    } catch (Exception e) {
      // ignore
    }
  }

  private void flushResultSet(ResultSet resultSet) throws UserInterruptException {
    initDateFormat();
    initJsonConfig();

    Map<String, Integer> width =
        ODPSConsoleUtils.getDisplayWidth(resultSet.getTableSchema().getColumns(), null, null);
    String frame = ODPSConsoleUtils.makeOutputFrame(width);
    String title = ODPSConsoleUtils.makeTitle(resultSet.getTableSchema().getColumns(), width);

    getContext().getOutputWriter().writeResult(frame);
    getContext().getOutputWriter().writeResult(title);
    getContext().getOutputWriter().writeResult(frame);

    while (resultSet.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Record record = resultSet.next();
      getContext().getOutputWriter().writeResult(formatRecord(record, width));
    }

    getContext().getOutputWriter().writeResult(frame);
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public boolean isSelectCommand() {
    return isSelectCommand;
  }

  public QueryCommand(boolean isSelect, String commandText, ExecutionContext context) {
    super(commandText, context);
    isSelectCommand = isSelect;
  }

  /**
   * 通过传递的参数，解析出对应的command
   */
  public static QueryCommand parse(String commandString, ExecutionContext sessionContext) {

    boolean isSelect = false;
    commandString = commandString.trim();

    if (commandString.toUpperCase().matches("^SELECT[\\s\\S]*")) {
      isSelect = true;
    }

    if (!commandString.endsWith(";")) {
      commandString = commandString + ";";
    }
    return new QueryCommand(isSelect, commandString, sessionContext);
  }
}

package com.aliyun.openservices.odps.console.commands;

import java.io.File;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.QueryUtil;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;
import lombok.Getter;

/**
 * Created by zhenhong.gzh on 16/1/14.
 */
public class ExecuteScriptCommand extends MultiClusterCommandBase {

  public final String filename;

  public ExecuteScriptCommand(String commandText, ExecutionContext context, String filename) {
    super(commandText, context);
    this.filename = filename;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    parseSettings();

    SQLTask task = new SQLTask();
    task.setName("console_sqlscript_task_" + Calendar.getInstance().getTimeInMillis());
    task.setQuery(getCommandText());

    addTaskSettings(task);

    runJob(task);
  }

  void parseSettings() throws ODPSConsoleException {
    List<String> cmds = new AntlrObject(getCommandText().trim()).splitCommands();
    int i = 0;
    for (; i < cmds.size(); i++) {
      String cmd = cmds.get(i).trim();

      if (cmd.isEmpty()) {
        continue;
      }

      try {
        SetCommand setCommand = SetCommand.parse(cmd, getContext());
        if (setCommand == null) {
          break;
        }
        setCommand.run();
      } catch (ODPSConsoleException | OdpsException e) {
        break;
      }
    }
  }

  private void addTaskSettings(SQLTask task) {
    Map<String, String> settings = new HashMap<String, String>();

    settings.put("odps.sql.submit.mode", "script");
    settings.put("odps.sql.script.filepath", filename);

    // odps optimize 2.0 ddl 2.0 etc
    // S26 need
    settings.put("odps.sql.planner.mode", "lot");
    settings.put("odps.sql.planner.parser.odps2", "true");
    settings.put("odps.sql.preparse.odps2", "lot");
    settings.put("odps.sql.sqltask.odps2", "true");
    settings.put("odps.sql.ddl.odps2", "true");
    settings.put("odps.compiler.output.format", "lot,pot");
    settings.put("odps.sql.runtime.mode", "executionengine");

    if (!getContext().isMachineReadable()) {
      settings.put("odps.sql.select.output.format", "HumanReadable");
    }

    settings.putAll(SetCommand.setMap);

    HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

    addSetting(taskConfig, settings);

    for (Map.Entry<String, String> property : taskConfig.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }
  }

  public static ExecuteScriptCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String option = "-s";

    if (optionList.contains(option)) {
      if (optionList.indexOf(option) + 1 < optionList.size()) {
        int index = optionList.indexOf(option);

        String filename = optionList.get(index + 1);

        optionList.remove(optionList.indexOf(option));
        optionList.remove(optionList.indexOf(filename));

        String cmd = FileUtil.getStringFromFile(filename);

        if (!cmd.trim().equals("")) {
          return new ExecuteScriptCommand(cmd, sessionContext, new File(filename).getAbsolutePath());
        }

      }
    }

    return null;
  }

}


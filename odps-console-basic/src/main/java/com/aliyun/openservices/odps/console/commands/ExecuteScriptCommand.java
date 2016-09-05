package com.aliyun.openservices.odps.console.commands;

import java.io.File;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

/**
 * Created by zhenhong.gzh on 16/1/14.
 */
public class ExecuteScriptCommand extends MultiClusterCommandBase {

  private String filename;

  public String getFilename() {
    return filename;
  }

  public ExecuteScriptCommand(String commandText, ExecutionContext context, String filename) {
    super(commandText, context);
    this.filename = filename;

  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    SQLTask task = new SQLTask();
    task.setName("console_sqlscript_task_" + Calendar.getInstance().getTimeInMillis());
    task.setQuery(getCommandText());

    addTaskSettings(task);

    runJob(task);
  }

  private void addTaskSettings(SQLTask task) {
    Map<String, String> settings = new HashMap<String, String>();

    settings.put("odps.sql.submit.mode", "script");
    settings.put("odps.sql.script.filepath", filename);

    settings.put("odps.sql.planner.mode", "lot");
    settings.put("odps.sql.planner.parser.odps2", "true");
    settings.put("odps.sql.preparse.odps2", "lot");

    if (!getContext().isMachineReadable()) {
      settings.put("odps.sql.select.output.format", "HumanReadable");
    }

    HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

    addSetting(taskConfig, settings);

    for (Map.Entry<String, String> property : taskConfig.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static ExecuteScriptCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    // 处理script的执行
    // parse -s参数
    String option = "-s";

    if (optionList.contains(option)) {
      if (optionList.indexOf(option) + 1 < optionList.size()) {
        int index = optionList.indexOf(option);
        // 创建相应的command列表
        String filename = optionList.get(index + 1);

        // 消费掉-s 及参数
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


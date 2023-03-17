package com.aliyun.openservices.odps.console.pub;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.common.CommandUtils;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class ShowAutoMvCommand extends AbstractCommand {

  private static final Pattern
      SHOW_PATTERN =
      Pattern.compile("\\s*SHOW\\s+AUTOMVMETA\\s*", Pattern.CASE_INSENSITIVE);

  public ShowAutoMvCommand(String commandText,
                           ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    Map<String, String> map = odps.projects().get().showAutoMvMeta();

    if (map == null || map.isEmpty()) {
      return;
    }

    DefaultOutputWriter writer = getContext().getOutputWriter();

    writer.writeResult("");

    if (map.containsKey("fileSize")) {
      writer.writeResult("fileSize: " + Long.parseLong(map.get("fileSize")));
    }

    if (map.containsKey("tableNum")) {
      writer.writeResult("num: " + Long.parseLong(map.get("tableNum")));
    }

    if (map.containsKey("updateTime")) {
      String updateTime = map.get("updateTime");
      writer.writeResult("updateTime: " + (updateTime.equals("-1") ? "-1"
                                                                   : CommandUtils.longToDateTime(
                                                                       updateTime)));
    }

    if (map.containsKey("lastAutoMvCreationStartTime")) {
      String lastAutoMvCreationStartTime = map.get("lastAutoMvCreationStartTime");
      writer.writeResult(
          "lastAutoMvCreationStartTime: " + (lastAutoMvCreationStartTime.equals("-1") ? "-1"
                                                                                      : CommandUtils.longToDateTime(
                                                                                          lastAutoMvCreationStartTime)));
    }

    if (map.containsKey("lastAutoMvCreationFinishTime")) {
      String lastAutoMvCreationFinishTime = map.get("lastAutoMvCreationFinishTime");
      writer.writeResult(
          "lastAutoMvCreationFinishTime: " + (lastAutoMvCreationFinishTime.equals("-1") ? "-1"
                                                                                        : CommandUtils.longToDateTime(
                                                                                            lastAutoMvCreationFinishTime)));
    }

    writer.writeResult("\nOK");
  }

  public static ShowAutoMvCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    Matcher matcher = SHOW_PATTERN.matcher(commandString);

    if (!matcher.matches()) {
      return null;
    }

    return new ShowAutoMvCommand(commandString, sessionContext);
  }
}

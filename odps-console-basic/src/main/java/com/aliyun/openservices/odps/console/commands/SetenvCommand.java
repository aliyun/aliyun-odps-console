package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SetenvCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"setenv"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: setenv <key>=<value>");
  }

  private static final LinkedHashMap<String, String> SETTINGS = new LinkedHashMap<>();
  private final String key;
  private final String value;

  public SetenvCommand(String key, String value, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.key = key;
    this.value = value;
    SETTINGS.put(key, value);
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {
    SetCommand setCommand = new SetCommand(true, key, value, getCommandText(), getContext());
    try {
      setCommand.run();
    } catch (Exception e) {
      SETTINGS.remove(key);
      throw e;
    }
  }

  public static void reset(String commandText, ExecutionContext context)
      throws ODPSConsoleException, OdpsException {
    for (Map.Entry<String, String> entry : SETTINGS.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue();
      SetCommand setCommand = new SetCommand(true, k, v, commandText, context);
      setCommand.run();
    }
  }

  public static boolean unset(String key) {
    return SETTINGS.remove(key) != null;
  }

  public static LinkedHashMap<String, String> getSettings() {
    return SETTINGS;
  }

  public static SetenvCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    if (commandString.toUpperCase().matches("^SETENV\\s+\\S+\\s*=\\s*\\S+.*")) {
      String keyValue = commandString.substring(6).trim();
      String[] temp = keyValue.split("=", 2);
      if (temp.length == 2) {
        return new SetenvCommand(temp[0].trim(), temp[1].trim(), commandString,
                                 sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
    }
    return null;
  }
}

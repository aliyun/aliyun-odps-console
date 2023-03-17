package com.aliyun.openservices.odps.console.pub;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class TriggerAutoMvCommand extends AbstractCommand {

  private static final Pattern
      TRIGGER_PATTERN =
      Pattern.compile("\\s*TRIGGER\\s+AUTOMVCREATION\\s*", Pattern.CASE_INSENSITIVE);


  public TriggerAutoMvCommand(String commandText,
                              ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    boolean success = odps.projects().get().triggerAutoMvCreation();

    if (success) {
      DefaultOutputWriter writer = getContext().getOutputWriter();
      writer.writeResult("\nOK");
    }
  }

  public static TriggerAutoMvCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    Matcher matcher = TRIGGER_PATTERN.matcher(commandString);

    if (!matcher.matches()) {
      return null;
    }

    return new TriggerAutoMvCommand(commandString, sessionContext);
  }
}

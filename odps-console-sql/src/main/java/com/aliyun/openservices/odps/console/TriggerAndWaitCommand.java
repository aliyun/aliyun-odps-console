package com.aliyun.openservices.odps.console;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import java.io.PrintStream;
import java.util.Iterator;

import static com.aliyun.openservices.odps.console.QueryCommand.PMC_TASK_NAME;

public class TriggerAndWaitCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"triggerandwait"};

  private String instanceId;

  private boolean waitComplete;

  public TriggerAndWaitCommand(String commandText, ExecutionContext context, String instanceId, boolean waitComplete) {
    super(commandText, context);
    this.instanceId = instanceId;
    this.waitComplete = waitComplete;
  }

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: triggerandwait [instanceID] [-async]");
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    Instance instance = odps.instances().get(instanceId);
    Instance.SetInformationResult result = instance.setInformation(PMC_TASK_NAME,
        "odps_progressive_data_fully_arrived", "true");
    if (!result.status.equalsIgnoreCase("ok")) {
      throw new ODPSConsoleException("trigger last round failed: [" + result.result + ", " + result.status + "]");
    }
    getWriter().writeResult(result.status.trim());

    if (waitComplete) {
      ExecutionContext context = getContext();
      try {
        InstanceRunner runner = new InstanceRunner(odps, instance, context);
        runner.waitForCompletion();
        Iterator<String> queryResult = runner.getResult();
        DefaultOutputWriter writer = context.getOutputWriter();

        if (queryResult != null) {
          while (queryResult.hasNext()) {
            writer.writeResult(queryResult.next());
          }
        }
      } finally {
        QueryUtil.printSubQueryLogview(odps, instance, PMC_TASK_NAME, context);
        if (OdpsHooks.isEnabled()) {
          OdpsHooks hooks = new OdpsHooks();
          hooks.after(instance, odps);
        }
      }
    }
  }

  public static TriggerAndWaitCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (commandString.trim().toUpperCase().startsWith("TRIGGERANDWAIT")) {
      String[] temp = commandString.trim().replaceAll("\\s+", " ").split(" ");
      if (temp.length >= 2) {
        boolean waitComplete = true;
        if (temp.length >=3) {
          waitComplete = !("-async".equalsIgnoreCase(temp[2]));
        }
        return new TriggerAndWaitCommand(commandString, sessionContext, temp[1], waitComplete);
      }
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid parameters]");
    }
    return null;
  }
}

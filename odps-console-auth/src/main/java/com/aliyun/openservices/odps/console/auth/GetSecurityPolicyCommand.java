package com.aliyun.openservices.odps.console.auth;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import java.io.PrintStream;

public class GetSecurityPolicyCommand extends AbstractCommand {
  private static final String REGEX = "GET\\s+SECURITY\\s+POLICY";

  public static final String[] HELP_TAGS = {"GET", "SECURITY", "POLICY"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: GET SECURITY POLICY");
  }

  public GetSecurityPolicyCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();

    String policy;
    policy =sm.getSecurityPolicy();
    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    outputWriter.writeResult(policy);
  }

  public static GetSecurityPolicyCommand parse(String commandText, ExecutionContext context) {
    commandText = commandText.trim().toUpperCase();
    if (commandText.matches(REGEX)) {
      return new GetSecurityPolicyCommand(commandText, context);
    }

    return null;
  }

}

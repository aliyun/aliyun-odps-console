package com.aliyun.openservices.odps.console.auth;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import java.io.PrintStream;

public class PutSecurityPolicyCommand extends AbstractCommand {
  private static final String REGEX = "PUT\\s+SECURITY\\s+POLICY\\s+.+";

  public static final String[] HELP_TAGS = {"PUT", "SECURITY", "POLICY"};

  private String policyPath;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: PUT SECURITY POLICY");
  }

  public PutSecurityPolicyCommand(String policyPath, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.policyPath = policyPath;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();

    String policy = FileUtil.getStringFromFile(policyPath);
    sm.putSecurityPolicy(policy);
    getWriter().writeError("OK");
  }

  public static PutSecurityPolicyCommand parse(String commandText, ExecutionContext context) {
    commandText = commandText.trim();
    if (commandText.toUpperCase().matches(REGEX)) {
      String[] splits = commandText.split("\\s+");
      return new PutSecurityPolicyCommand(splits[3], commandText, context);
    }

    return null;
  }
}

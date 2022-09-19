package com.aliyun.openservices.odps.console.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class CommandWithOptionP {

  public static String SHORT_PROJECT_OPT = "p";
  public static String LONG_PROJECT_OPT = "project";

  private String cmd;
  private CommandLine commandLine;

  public CommandWithOptionP(String cmdTxt) throws ODPSConsoleException {
    String[] args = CommandParserUtils.getCommandTokens(cmdTxt);
    Options opts = getProjectOptions();
    commandLine = CommandParserUtils.getCommandLine(args, opts);
    cmd = String.join(" ", commandLine.getArgs());
  }

  public boolean hasOptionP() {
    return commandLine.hasOption(SHORT_PROJECT_OPT);
  }

  public String getProjectValue() {
    return commandLine.getOptionValue(SHORT_PROJECT_OPT);
  }

  public String[] getArgs() {
    return commandLine.getArgs();
  }

  public String getCmd() {
    return cmd;
  }

  private static Options getProjectOptions() {
    Options opts = new Options();
    Option projectName = new Option(
        SHORT_PROJECT_OPT, LONG_PROJECT_OPT, true, "project name");
    projectName.setRequired(false);
    opts.addOption(projectName);
    return opts;
  }
}

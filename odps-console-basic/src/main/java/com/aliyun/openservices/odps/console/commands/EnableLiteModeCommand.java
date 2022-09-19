package com.aliyun.openservices.odps.console.commands;

import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class EnableLiteModeCommand extends AbstractCommand {

  private static final String LITE_MODE = "--lite";

  public EnableLiteModeCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    getContext().setLiteMode(true);
  }

  public static EnableLiteModeCommand parse(List<String> optionList,
                                            ExecutionContext sessionContext) {
    if (ODPSConsoleUtils.shiftBooleanOption(optionList, LITE_MODE)) {
      return new EnableLiteModeCommand(LITE_MODE, sessionContext);
    }

    return null;
  }
}

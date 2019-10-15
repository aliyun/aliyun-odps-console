package com.aliyun.openservices.odps.console.commands;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import java.util.List;

/**
 * This class is used to get application access id/key. The id/key will be used to sign http
 * requests and tokens.
 *
 * @author Jon (wangzhong.zw@alibaba-inc.com)
 */
public class AppLoginCommand extends AbstractCommand {

  private static final String APP_ACCESS_ID = "--app-id";
  private static final String APP_ACCESS_KEY = "--app-key";

  private String appAccessId;
  private String appAccessKey;

  public AppLoginCommand(String appAccessId, String appAccessKey, String commandText,
                         ExecutionContext context) {
    super(commandText, context);
    this.appAccessId = appAccessId;
    this.appAccessKey = appAccessKey;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    if (appAccessId != null) {
      getContext().setAppAccessId(appAccessId);
    }
    if (appAccessKey != null) {
      getContext().setAppAccessKey(appAccessKey);
    }
  }

  public static AppLoginCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String appAccessId = ODPSConsoleUtils.shiftOption(optionList, APP_ACCESS_ID);
    String appAccessKey = ODPSConsoleUtils.shiftOption(optionList, APP_ACCESS_KEY);
    if (appAccessId != null && appAccessKey != null) {
      return new AppLoginCommand(
          appAccessId, appAccessKey, APP_ACCESS_ID + APP_ACCESS_KEY, sessionContext);
    }
    return null;
  }
}

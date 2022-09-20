package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class UseQuotaCommand extends AbstractCommand {

  private static final String OPTION_REGION_ID = "--quota-region-id";
  private static final String OPTION_QUOTA_NAME = "--quota-name";
  private static final String OPTION_DEFAULT_QUOTA_FLAG = "default";

  public static final String[] HELP_TAGS = new String[] {"use", "quota"};
  private static final Pattern PATTERN = Pattern.compile(
"USE\\s+QUOTA\\s+([\\u4E00-\\u9FA5A-Za-z0-9_\\-]+)(\\s+IN\\sREGION\\s(.+))?",
      Pattern.CASE_INSENSITIVE);

  public static void printUsage(PrintStream stream) {
    stream.println("Usage:");
    stream.println("  use quota <quota name>[ in region <region id>];");
    stream.println("Examples:");
    stream.println("  use quota my_quota;");
    stream.println("  use quota my_quota in region cn-beijing;");
  }

  private String regionId;
  private String quotaName;

  public UseQuotaCommand(
      String commandText,
      ExecutionContext context,
      String regionId,
      String quotaName) {
    super(commandText, context);
    this.regionId = regionId;
    this.quotaName = quotaName;
  }

  @Override
  protected void run() throws ODPSConsoleException, OdpsException {

    if (this.quotaName == null || this.quotaName.isEmpty()) {
      throw new InvalidParameterException("Invalid parameter: Quota name is empty.");
    }

    if (this.quotaName.equalsIgnoreCase(OPTION_DEFAULT_QUOTA_FLAG)) {
      SetCommand.setMap.remove("odps.task.wlm.quota");
      getContext().setQuotaName("");
      getContext().setQuotaRegionId("");
      return;
    }

    if (this.regionId == null || this.regionId.isEmpty()) {
      this.regionId = getCurrentOdps().projects().get(getCurrentProject()).getDefaultQuotaRegion();
    }

    // Make sure the quota exists
    Quota quota = null;
    try {
      quota = getCurrentOdps().quotas().get(regionId, quotaName);
      quota.reload();
      if (quota.isParentQuota()) {
        throw new InvalidParameterException("Level 1 quota is not allowed to use. Please use a Level 2 quota.");
      }
      if (quota.getResourceSystemType() != null
          && quota.getResourceSystemType().equalsIgnoreCase("FUXI_ONLINE")) {
        throw new InvalidParameterException("Online quota is not allowed to use manually. " +
                "It can only be used automatically by entering interactive mode.");
      }
    } catch (NoSuchObjectException e) {
      String errMsg = "Quota " + quotaName + " is not found in region " + regionId
              + ". It may be in another region or not exist at all.";
      NoSuchObjectException ee = new NoSuchObjectException(errMsg, e);
      ee.setStatus(e.getStatus());
      ee.setRequestId(e.getRequestId());
      throw ee;
    } catch (OdpsException e) {
      String errMsg = "Read quota " + quotaName + " in region " + regionId + " failed.";
      OdpsException ee = new OdpsException(errMsg, e);
      ee.setStatus(e.getStatus());
      ee.setRequestId(e.getRequestId());
      throw ee;
    }

    String value = String.format("%s@%s", quota.getNickname(), quota.getRegionId());
    SetCommand.setMap.put("odps.task.wlm.quota", value);
    getContext().setQuotaName(quota.getNickname());
    getContext().setQuotaRegionId(quota.getRegionId());
  }

  public static UseQuotaCommand parse(List<String> optionList, ExecutionContext sessionContext) {
    String regionId = ODPSConsoleUtils.shiftOption(optionList, OPTION_REGION_ID);
    String quotaName = ODPSConsoleUtils.shiftOption(optionList, OPTION_QUOTA_NAME);
    if (!StringUtils.isNullOrEmpty(quotaName)) {
      return new UseQuotaCommand("", sessionContext, regionId, quotaName);
    }
    return null;
  }

  public static UseQuotaCommand parse(String commandString, ExecutionContext sessionContext) {
    Matcher matcher = PATTERN.matcher(commandString);
    if (matcher.matches()) {
      return new UseQuotaCommand(
          commandString,
          sessionContext,
          matcher.group(3),
          matcher.group(1));
    }
    return null;
  }
}

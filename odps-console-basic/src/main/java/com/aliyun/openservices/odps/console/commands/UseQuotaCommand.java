package com.aliyun.openservices.odps.console.commands;

import java.io.IOException;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.FileStorage;
import com.aliyun.openservices.odps.console.utils.LocalCacheUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.SessionUtils;

public class UseQuotaCommand extends AbstractCommand {

  private static final String OPTION_REGION_ID = "--quota-region-id";
  private static final String OPTION_QUOTA_NAME = "--quota-name";
  private static final String OPTION_DEFAULT_QUOTA_FLAG = "default";

  public static final String[] HELP_TAGS = new String[]{"use", "quota"};
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
  public void run() throws ODPSConsoleException, OdpsException {

    if (this.quotaName == null || this.quotaName.isEmpty()) {
      throw new InvalidParameterException("Invalid parameter: Quota name is empty.");
    }

    if (this.quotaName.equalsIgnoreCase(OPTION_DEFAULT_QUOTA_FLAG)) {
      SetCommand.setMap.remove("odps.task.wlm.quota");
      getContext().setQuotaName("");
      getContext().setQuotaRegionId("");
      if (getContext().isMcqaV2()) {
        getContext().setMcqaV2(false);
        getContext().setInteractiveQuery(false);
        SessionUtils.clearSessionContext(getContext());
      }
      getContext().getOutputWriter()
          .writeError("Clear quota successfully.");
      return;
    }

    // Make sure the quota exists
    try {
      QuotaCacheItem quota = load(quotaName);
      if (quota == null) {
        getContext().getOutputWriter()
            .writeError("Cannot use quota " + quotaName);
        return;
      }

      if (quota.isParentQuota) {
        throw new InvalidParameterException(
            "Level 1 quota is not allowed to use. Please use a Level 2 quota.");
      }
      if ("FUXI_ONLINE".equalsIgnoreCase(quota.quotaType)) {
        throw new InvalidParameterException("Online quota is not allowed to use manually. " +
                                            "It can only be used automatically by entering interactive mode.");
      }
      // fuxi_vw means enable query by mcqa v2
      if ("FUXI_VW".equalsIgnoreCase(quota.quotaType)) {
        getContext().setInteractiveQuery(true);
        getContext().setMcqaV2(true);
        SessionUtils.resetSQLExecutor(null, null, getContext(), getCurrentOdps(), false,
                                      quota.quotaName, true, quota.mcqaV2Header, regionId);
        SetCommand.setMap.put(ODPSConsoleConstants.ODPS_TASK_WLM_QUOTA, quota.quotaName);
        SetCommand.setMap.put(ODPSConsoleConstants.HTTP_SUBMIT_HEADERS, "x-odps-mcqa-conn=" + quota.mcqaV2Header + ",");
      } else {
        // mcqa v2 no need to set hints
        String value = String.format("%s@%s", quota.quotaName, quota.regionId);
        SetCommand.setMap.put(ODPSConsoleConstants.ODPS_TASK_WLM_QUOTA, value);
      }
      getContext().setQuotaName(quota.quotaName);
      getContext().setQuotaRegionId(quota.regionId);
      if (StringUtils.isNullOrEmpty(regionId)) {
        getContext().getOutputWriter()
            .writeError("Use quota " + quotaName + " successfully.");
      } else {
        getContext().getOutputWriter()
            .writeError("Use quota " + quotaName + " in region " + regionId + " successfully.");
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
    } catch (IOException e) {
      String errMsg = "Read quota " + quotaName + " in region " + regionId + " from cache failed, because ";
      throw new ODPSConsoleException(errMsg + e.getMessage(), e);
    }
  }

  public static UseQuotaCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException, OdpsException {
    String regionId = ODPSConsoleUtils.shiftOption(optionList, OPTION_REGION_ID);
    String quotaName = ODPSConsoleUtils.shiftOption(optionList, OPTION_QUOTA_NAME);
    if (!StringUtils.isNullOrEmpty(quotaName)) {
      return new UseQuotaCommand("", sessionContext, regionId, quotaName);
    }
    return null;
  }

  public static UseQuotaCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException, OdpsException {
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

  // 在文件中缓存 quota 相关信息，缓存地址为
  // configDir/.quotas/hex(endpoint_projectName_accessId)/quotaName.cache
  static class QuotaCacheItem {
    String regionId;
    String quotaName;
    String quotaType;
    String mcqaV2Header;
    boolean isParentQuota;
    Instant expirationTime;
  }

  private static String quotaCacheCategory = "quotas";

  private QuotaCacheItem load(String quotaName)
      throws OdpsException, ODPSConsoleException, IOException {
    if (StringUtils.isNullOrEmpty(quotaName)) {
      return null;
    }
    QuotaCacheItem quotaCacheItem = null;
    if (getContext().isEnableQuotaCache()) {
      String cacheFile =
          LocalCacheUtils.getSpecificCacheFile(getContext(), quotaCacheCategory, quotaName);
      FileStorage<QuotaCacheItem> cache = new FileStorage<>(cacheFile, QuotaCacheItem.class);
      quotaCacheItem = cache.load();
      if (quotaCacheItem != null && quotaCacheItem.expirationTime != null
          && quotaCacheItem.expirationTime.isBefore(Instant.now())) {
        quotaCacheItem = null;
      }
    }
    if (quotaCacheItem == null || (this.regionId != null && !this.regionId.equals(
        quotaCacheItem.regionId))) {
      if (this.regionId == null || this.regionId.isEmpty()) {
        this.regionId =
            getCurrentOdps().projects().get(getCurrentProject()).getDefaultQuotaRegion();
      }
      if (quotaName.startsWith("temp_")) {
        quotaCacheItem = new QuotaCacheItem();
        quotaCacheItem.quotaName = quotaName;
        quotaCacheItem.regionId = "cn";
        quotaCacheItem.quotaType = "FUXI_VW";
        quotaCacheItem.mcqaV2Header = quotaName.substring(5);
        quotaCacheItem.isParentQuota = false;
        quotaCacheItem.expirationTime = Instant.now().plus(1, ChronoUnit.DAYS);
      } else {
        Quota quota = getCurrentOdps().quotas().get(this.regionId, quotaName);
        quota.reload();
        quotaCacheItem = new QuotaCacheItem();
        quotaCacheItem.quotaName = quotaName;
        quotaCacheItem.regionId = quota.getRegionId();
        quotaCacheItem.quotaType = quota.getResourceSystemType();
        quotaCacheItem.mcqaV2Header = quota.getMcqaConnHeader();
        quotaCacheItem.isParentQuota = quota.isParentQuota();
        quotaCacheItem.expirationTime = Instant.now().plus(1, ChronoUnit.DAYS);
      }
      save(quotaCacheItem);
    }
    return quotaCacheItem;
  }

  private void save(QuotaCacheItem quotaCacheItem) throws IOException {
    if (getContext().isEnableQuotaCache()) {
      String cacheFile =
          LocalCacheUtils.getSpecificCacheFile(getContext(), quotaCacheCategory,
                                               quotaCacheItem.quotaName);
      FileStorage<QuotaCacheItem> cache = new FileStorage<>(cacheFile, QuotaCacheItem.class);
      cache.save(quotaCacheItem);
    }
  }
}

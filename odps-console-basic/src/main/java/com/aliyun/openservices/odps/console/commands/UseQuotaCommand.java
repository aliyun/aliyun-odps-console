package com.aliyun.openservices.odps.console.commands;

import java.io.IOException;
import java.io.PrintStream;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.sqa.v2.MaxQAConnInfo;
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
  private static final String OPTION_FALLBACK_QUOTANAME = "--fallback_quotaname";
  private static final String OPTION_INTERACTIVE_AUTO_FALLBACK = "--interactive_auto_fallback";
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
  private String fallbackQuotaName;
  private boolean interactiveAutoFallback;

  public UseQuotaCommand(
      String commandText,
      ExecutionContext context,
      String regionId,
      String quotaName,
      String fallbackQuotaName,
      boolean interactiveAutoFallback) {
    super(commandText, context);
    this.regionId = regionId;
    this.quotaName = quotaName;
    this.fallbackQuotaName = fallbackQuotaName;
    this.interactiveAutoFallback = interactiveAutoFallback;
  }

  @Override
  public void run() throws ODPSConsoleException, OdpsException {
    if (this.quotaName == null || this.quotaName.isEmpty()) {
      throw new InvalidParameterException("Invalid parameter: Quota name is empty.");
    }

    // Mark that quota is specified from command line
    getContext().setQuotaSpecifiedFromCommandLine(true);

    if (StringUtils.isNotBlank(fallbackQuotaName)) {
      getContext().setFallbackQuotaName(fallbackQuotaName);
    }

    if (interactiveAutoFallback) {
      getContext().setInteractiveAutoFallback(true);
    }

    if (this.quotaName.equalsIgnoreCase(OPTION_DEFAULT_QUOTA_FLAG)) {
      SetCommand.setMap.remove("odps.task.wlm.quota");
      getContext().setQuotaName("");
      getContext().setQuotaRegionId("");
      if (getContext().isMcqaV2()) {
        getContext().setMcqaV2(false);
        SessionUtils.clearSessionContext(getContext());
      }
      getContext().getOutputWriter()
          .writeError("Clear quota successfully.");
      return;
    }

    if (!getContext().isInteractiveQuery()) {
      SetCommand.setMap.put(ODPSConsoleConstants.ODPS_TASK_WLM_QUOTA, quotaName);
      getContext().setQuotaName(quotaName);
      getContext().setQuotaRegionId(regionId);
      return;
    }

    // Make sure the quota exists
    try {
      // no exception just means maxqa quota
      QuotaCacheItem quota = load(quotaName);
      if (quota == null) {
        getContext().getOutputWriter()
            .writeError("Cannot use quota " + quotaName);
        return;
      }
      SessionUtils.initMaxQASession(getContext(), MaxQAConnInfo.builder()
        .quotaName(quota.quotaName)
        .regionId(quota.regionId)
        .connInfo(quota.mcqaV2Header)
        .build());

      if (StringUtils.isNullOrEmpty(regionId)) {
        getContext().getOutputWriter()
          .writeError("Use MaxQA quota " + quotaName + " successfully.");
      } else {
        getContext().getOutputWriter()
            .writeError("Use quota " + quotaName + " in region " + regionId + " successfully.");
      }
    } catch (OdpsException debugE) {
      getContext().getOutputWriter().writeDebug(debugE);
    } catch (IOException e) {
      String errMsg = "Read quota " + quotaName + " in region " + regionId + " from cache failed, because ";
      throw new ODPSConsoleException(errMsg + e.getMessage(), e);
    } finally {
      SetCommand.setMap.put(ODPSConsoleConstants.ODPS_TASK_WLM_QUOTA, quotaName);
      getContext().setQuotaName(quotaName);
      getContext().setQuotaRegionId(regionId);
    }
  }

  public static UseQuotaCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException, OdpsException {
    String regionId = ODPSConsoleUtils.shiftOption(optionList, OPTION_REGION_ID);
    String quotaName = ODPSConsoleUtils.shiftOption(optionList, OPTION_QUOTA_NAME);
    String fallbackQuotaName = ODPSConsoleUtils.shiftOption(optionList, OPTION_FALLBACK_QUOTANAME);
    String interactiveAutoFallbackStr = ODPSConsoleUtils.shiftOption(optionList, OPTION_INTERACTIVE_AUTO_FALLBACK);
    boolean interactiveAutoFallback = false;
    if (!StringUtils.isNullOrEmpty(interactiveAutoFallbackStr)) {
      interactiveAutoFallback = Boolean.parseBoolean(interactiveAutoFallbackStr);
    }
    if (fallbackQuotaName == null) {
      fallbackQuotaName = "";
    }
    if (!StringUtils.isNullOrEmpty(quotaName)) {
      // Mark that quota is specified from command line immediately during parsing,
      // so that UseProjectCommand can check this flag before UseQuotaCommand.run() is called
      sessionContext.setQuotaSpecifiedFromCommandLine(true);
      return new UseQuotaCommand("", sessionContext, regionId, quotaName, fallbackQuotaName, interactiveAutoFallback);
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
          matcher.group(1),
          "",
          false);
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
    Long expirationTime;
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
      try {
        quotaCacheItem = cache.load();
      } catch (IOException ignored) {
      }
      if (quotaCacheItem != null && quotaCacheItem.expirationTime != null
          && Instant.ofEpochMilli(quotaCacheItem.expirationTime).isBefore(Instant.now())) {
        quotaCacheItem = null;
      }
    }
    if (quotaCacheItem == null || (this.regionId != null && !this.regionId.equals(
        quotaCacheItem.regionId))) {
      if (quotaName.startsWith("temp_")) {
        quotaCacheItem = new QuotaCacheItem();
        quotaCacheItem.quotaName = quotaName;
        quotaCacheItem.regionId = "cn";
        quotaCacheItem.quotaType = "FUXI_VW";
        quotaCacheItem.mcqaV2Header = quotaName.substring(5);
        quotaCacheItem.expirationTime = Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli();
      } else {
        MaxQAConnInfo maxQAConnInfo = getCurrentOdps().quotas().getMaxQAConnInfo(quotaName);

        quotaCacheItem = new QuotaCacheItem();
        quotaCacheItem.quotaName = quotaName;
        quotaCacheItem.regionId = maxQAConnInfo.getRegionId();
        quotaCacheItem.quotaType = "FUXI_VW";
        quotaCacheItem.mcqaV2Header = maxQAConnInfo.getConnInfo();
        quotaCacheItem.expirationTime = Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli();
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

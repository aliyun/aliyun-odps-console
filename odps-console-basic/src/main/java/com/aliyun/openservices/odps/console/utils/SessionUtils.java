package com.aliyun.openservices.odps.console.utils;

import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.sqa.v2.FallbackInfo;
import com.aliyun.odps.sqa.v2.MaxQAConnInfo;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * Created by dongxiao on 2019/12/23.
 */
public class SessionUtils {

  public static void initMaxQASession(ExecutionContext context, MaxQAConnInfo maxQAConnInfo)
    throws OdpsException {
    context.setMcqaV2(true);
    context.setAutoSessionMode(false);
    SessionUtils.resetSQLExecutor(null, null, context, context.getCurrentOdps(), false,
                                  maxQAConnInfo.getQuotaName(), true, maxQAConnInfo.getConnInfo(), maxQAConnInfo.getRegionId());
    SetCommand.setMap.put(ODPSConsoleConstants.HTTP_SUBMIT_HEADERS,
                          "x-odps-mcqa-conn=" + maxQAConnInfo.getConnInfo() + ",");
  }

  public static void autoAttachSession(ExecutionContext context, Odps odps) throws OdpsException,ODPSConsoleException {
    Map<String, String> predefinedSetCommands = context.getPredefinedSetCommands();
    String majorVersion = predefinedSetCommands.getOrDefault(ODPSConsoleConstants.TASK_MAJOR_VERSION, "");
    LocalCacheUtils.CacheItem sessionCache = context.getLocalCache();
    String sessionId = null;
    Instance instance = null;
    if (sessionCache != null && (sessionCache.projectName.equals(context.getProjectName()))
        && (sessionCache.sessionName.equals(context.getInteractiveSessionName())) && (sessionCache.majorVersion.equals(majorVersion))){
      sessionId = sessionCache.sessionId;
    }
    try {
      if (!StringUtils.isNullOrEmpty(sessionId)
          && odps.instances().exists(sessionId)) {
          instance = odps.instances().get(sessionId);
      }
    } catch (OdpsException e) {
      context.getOutputWriter().writeDebug(
          "Cached session not exist, id:" + sessionId);
    }
    String currentId = recoverSQLExecutor(instance, context, odps, true);
    if (currentId.equals(sessionId)) {
      context.getOutputWriter().writeDebug("Recover session id in cache, id:" + sessionId);
    } else {
      context.getOutputWriter().writeDebug(
          "Session in cache is not running now, attach session, cached id:"
              + sessionId + ", new session:" + currentId);
    }
    context.setLocalCache(new LocalCacheUtils.CacheItem(currentId, System.currentTimeMillis()/1000 , context.getProjectName(),
                                                        context.getInteractiveSessionName(), majorVersion));
  }

  public static String recoverSQLExecutor(Instance instance, ExecutionContext context, Odps odps, boolean autoReattach) throws OdpsException {
    String sessionName = context.getInteractiveSessionName();
    String quotaName = context.getQuotaName();
    return resetSQLExecutor(sessionName, instance, context, odps, autoReattach, quotaName);
  }

  public static String resetSQLExecutor(String sessionName, Instance instance, ExecutionContext context, Odps odps, boolean autoReattach, String quotaName) throws OdpsException {
    return resetSQLExecutor(sessionName, instance, context, odps, autoReattach, quotaName, false, null, null);
  }

  public static String resetSQLExecutor(String sessionName, Instance instance,
                                        ExecutionContext context, Odps odps, boolean autoReattach,
                                        String quotaName, boolean maxqa, String maxqaConnInfo,
                                        String regionId) throws OdpsException {
    SQLExecutorBuilder builder = new SQLExecutorBuilder();
    builder.odps(odps)
        .quotaName(quotaName)
        .serviceName(sessionName)
        .properties(SetCommand.setMap)
        .tunnelEndpoint(context.getTunnelEndpoint())
        .runningCluster(context.getRunningCluster())
        .fallbackPolicy(context.getFallbackPolicy())
        .useInstanceTunnel(context.isUseInstanceTunnel())
        .enableReattach(autoReattach)
        .taskName(ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME)
        .attachTimeout(context.getAttachTimeout())
        .recoverFrom(instance)
        .enableMaxQA(maxqa)
        .regionId(regionId);
    builder.setEnableTypedResult(false);
    if ("false".equalsIgnoreCase(
        SetCommand.setMap.getOrDefault("odps.sql.session.select.only", "true"))) {
      builder.sessionSupportNonSelect(true);
    }
    if (maxqa) {
      MaxQAConnInfo.Builder maxQAConnInfoBuilder = MaxQAConnInfo.builder();
      if (context.isInteractiveAutoFallback()) {
        String fallbackQuota = StringUtils.isNotBlank(context.getFallbackQuotaName())
            ? context.getFallbackQuotaName()
            : "default";
        maxQAConnInfoBuilder.fallbackInfo(FallbackInfo.enable(fallbackQuota));
      }
      maxQAConnInfoBuilder.connInfo(maxqaConnInfo).quotaName(quotaName).regionId(regionId);
      builder.maxQAConnInfo(maxQAConnInfoBuilder.build());
    }
    SQLExecutor executor = builder.build();
    String currentId = maxqa ? null : executor.getInstance().getId();
    resetSessionContext(executor, odps, context);
    return currentId;
  }

  public static void resetSessionContext(SQLExecutor executor, Odps odps, ExecutionContext context) {
    ExecutionContext.setExecutor(executor);
    if (executor == null) {
      context.setInteractiveQuery(false);
    } else {
      context.setInteractiveQuery(true);
      context.getOutputWriter().writeDebug(executor.getLogView());
    }
  }

  public static void clearSessionContext(ExecutionContext context) {
    resetSessionContext(null, null, context);
    if (context.getAutoSessionMode()) {
      LocalCacheUtils.clearCache();
    }
  }
}

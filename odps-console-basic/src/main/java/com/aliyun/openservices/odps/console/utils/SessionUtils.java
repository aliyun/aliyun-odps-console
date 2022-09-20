package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Quota;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

import java.util.TimeZone;

/**
 * Created by dongxiao on 2019/12/23.
 */
public class SessionUtils {
  public static void autoAttachSession(ExecutionContext context, Odps odps) throws OdpsException,ODPSConsoleException {
    LocalCacheUtils.CacheItem sessionCache = context.getLocalCache();
    String sessionId = null;
    Instance instance = null;
    if (sessionCache != null && (sessionCache.projectName.equals(context.getProjectName())) && (sessionCache.sessionName.equals(context.getInteractiveSessionName()))){
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
    context.setLocalCache(new LocalCacheUtils.CacheItem(currentId, System.currentTimeMillis()/1000 , context.getProjectName(), context.getInteractiveSessionName()));
  }

  public static String recoverSQLExecutor(Instance instance, ExecutionContext context, Odps odps, boolean autoReattach) throws OdpsException {
    String sessionName = context.getInteractiveSessionName();
    String quotaName = context.getQuotaName();
    return resetSQLExecutor(sessionName, instance, context, odps, autoReattach, quotaName);
  }

  public static String resetSQLExecutor(String sessionName, Instance instance, ExecutionContext context, Odps odps, boolean autoReattach, String quotaName) throws OdpsException {
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
        .recoverFrom(instance);
    SQLExecutor executor = builder.build();
    String currentId = executor.getInstance().getId();
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

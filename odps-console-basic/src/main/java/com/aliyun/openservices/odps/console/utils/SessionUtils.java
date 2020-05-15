package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Session;
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
    Session session = null;
    if (sessionCache != null) {
      if ((System.currentTimeMillis()/1000 - sessionCache.attachTime) > context.getSessionCacheExpireTime()) {
        context.getOutputWriter().writeDebug("Session in cache is expired, will attach session.");
      } else {
        String sessionId = sessionCache.sessionId;
        if (odps.instances().exists(sessionId)) {
          Instance instance = odps.instances().get(sessionId);
          if (instance.getStatus() == Instance.Status.RUNNING) {
            context.getOutputWriter().writeDebug("Use session id in cache, id:" + sessionCache.sessionId);
            session = new Session(odps, instance);
          } else {
            context.getOutputWriter().writeDebug(
                "Session in cache is not running now, will attach session, cached id:" + sessionCache.sessionId);
          }
        } else {
          context.getOutputWriter().writeDebug(
              "Session in cache is not running now, will attach session, cached id:" + sessionCache.sessionId);
        }
      }
    }
    // cache miss, attach new session
    if (session == null) {
      String sessionName = context.getInteractiveSessionName();

      session = Session.attach(
          OdpsConnectionFactory.createOdps(context),
          sessionName,
          SetCommand.setMap,
          0L,
          context.getRunningCluster(),
          ODPSConsoleConstants.SESSION_DEFAULT_TASK_NAME
      );
      context.getOutputWriter().writeDebug("Auto attach session, id:" + session.getInstance().getId());
      context.setLocalCache(new LocalCacheUtils.CacheItem(session.getInstance().getId(), System.currentTimeMillis()/1000));
    }
    ExecutionContext.setSessionInstance(session);
    context.setInteractiveQuery(true);
    // init timezone
    try {
      String timezode = odps.projects().get(context.getProjectName()).getProperty("odps.sql.timezone");
      if (StringUtils.isNullOrEmpty(timezode)) {
        timezode = TimeZone.getDefault().getID();
        context.getOutputWriter().writeDebug("Use default timezone:" + timezode);
      } else {
        context.getOutputWriter().writeDebug("Use project timezone:" + timezode);
      }
      context.setSqlTimezone(timezode);

    } catch (Exception e) {
      context.getOutputWriter().writeError(e.getMessage());
    }
  }
}

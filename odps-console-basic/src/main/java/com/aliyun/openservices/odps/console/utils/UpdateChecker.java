package com.aliyun.openservices.odps.console.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.Map;

import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;

/**
 * Created by zhenhong.gzh on 16/8/2.
 */
public class UpdateChecker {
  private static final int CONNECT_TIME_OUT = 5 * 1000;
  private static final int READ_TIME_OUT = 10 * 1000;
  private static final long UPDATE_PROMPT_INTERVAL_MILLIS = 2 * 24 * 60 * 60 * 1000; // 2 天

  private static final String updatePromptHistoryFile = ".odps_update";
  private static final String VERSION_SOURCE = "latest_version.json";
  private static final String SNAPSHOT_SUFFIX = "-snapshot";

  private String updateURL;
  private String onlineVersion;
  private String onlineDownloadURL;
  private ExecutionContext context;

  public UpdateChecker(String updateURL, ExecutionContext context) {
    this.updateURL = updateURL;
    this.context = context;
  }

  public String getOnlineDownloadURL() {
    if (StringUtils.isNullOrEmpty(onlineDownloadURL)) {
      getOnlineUpdateInfo();
    }

    return onlineDownloadURL;
  }

  public String getOnlineVersion() {
    if (StringUtils.isNullOrEmpty(onlineVersion)) {
      getOnlineUpdateInfo();
    }

    return onlineVersion;
  }

  public boolean shouldPromptUpdate() {
    return (shouldPrompt() && shouldUpdate());
  }

  private void getOnlineUpdateInfo() {
    final String onlineCheckURL = updateURL + "/" + VERSION_SOURCE;

    try {
      URL url = new URL(onlineCheckURL);
      URLConnection conn = url.openConnection();
      conn.setConnectTimeout(CONNECT_TIME_OUT);
      conn.setReadTimeout(READ_TIME_OUT);

      BufferedReader in = new BufferedReader(new InputStreamReader(
          conn.getInputStream(), "UTF-8"));
      String inputLine;
      StringBuilder content = new StringBuilder();
      while ((inputLine = in.readLine()) != null) {
        content.append(inputLine);
      }
      in.close();

      Map updateInfos = new GsonBuilder().disableHtmlEscaping().create()
              .fromJson(content.toString(), Map.class);
      Map node = ((Map)updateInfos.get("odpscmd"));


      onlineVersion = (String)node.get("version");
      onlineDownloadURL = updateURL + (String)node.get("path");
    } catch (Exception ignore) {
      context.getOutputWriter().writeDebug(ignore);
      // do nothing
    }
  }

  private boolean shouldUpdate() {
    String current = ODPSConsoleUtils.getMvnVersion();

    String onlineVersion = getOnlineVersion();

    if (!StringUtils.isNullOrEmpty(current) && !StringUtils.isNullOrEmpty(onlineVersion)) {
      try {
        if (ODPSConsoleUtils.compareVersion(current, onlineVersion) < 0) {
          return true;
        }
      } catch (Exception e) {
        context.getOutputWriter().writeDebug(e);
        // ignore
      }
    }

    return false;
  }

  private boolean shouldPrompt() {
    try {
      String path;
      if (!StringUtils.isNullOrEmpty(ODPSConsoleUtils.getConfigFilePath())) {
        path = new File(ODPSConsoleUtils.getConfigFilePath()).getParent();
      } else {
        path = System.getProperty("user.home");
      }

      path = path + File.separator + updatePromptHistoryFile;
      File file = new File(path);
      if (file.exists()) {
        String lastPromptDate = FileUtils.readFileToString(file, "utf-8");
        Date lastDate = ODPSConsoleUtils.DATE_FORMAT.parse(lastPromptDate);
        Date now = new Date();

        if (now.getTime() - lastDate.getTime() < UPDATE_PROMPT_INTERVAL_MILLIS) {
          return false;
        }
      } else {
        file.createNewFile();
      }

      FileUtils.write(file, ODPSConsoleUtils.formatDate(new Date()), "utf-8");
    } catch (Exception e) {
      context.getOutputWriter().writeDebug(e);
      // ignore file history failure
    }

    return true;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console.xflow;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.XFlows.XFlowInstance;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.PluginUtil;

/**
 * Created by yuanman.ym on 21/5/18.
 *
 * Jupyter 等算法轮询 Url 的 progress helper
 */
public class UrlPollingProgressHelper extends XFlowProgressHelper {
  private static int interval = 3000;

  public String getPAIAlgoName() {
    return "";
  }

  UrlPollingProgressHelper() {
    super.setInterval(interval);
  }

  private boolean connectedFlag = false;

  @Override
  public boolean needProgressMessage(Instance instance) {
    return !instance.isTerminated();
  }

  @Override
  public String getProgressMessage(Instance instance) throws ODPSConsoleException,
                                                             OdpsException {
    StringBuilder sb = new StringBuilder();

    Set<String> taskNames = instance.getTaskNames();

    StringWriter strWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(strWriter);
    int i = 0;
    for (String taskName : taskNames) {
      List<Instance.StageProgress> stages = instance.getTaskProgress(taskName);

      if (stages.size() == 0) {
        writer.print(taskName + ": " + "running");
      } else {
        writer.print(taskName + ": ");
        writer.print(Instance.getStageProgressFormattedString(stages));
      }
      if (++i < taskNames.size()) {
        writer.print(", ");
      }
    }

    String url = getConfig();
    if (url != null && !connectedFlag) {
      int code = 0;
      try {
        URL pollingUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection)pollingUrl
          .openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        code = connection.getResponseCode();
      } catch (IOException e) {
        throw new OdpsException(e.getMessage());
      }
      if (code == 200) {
        writer.print("\nurl: " + url);
        connectedFlag = true;
      }
    }

    String str = strWriter.toString();
    sb.append(str);
    return sb.toString();
  }

}

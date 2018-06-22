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
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.PluginUtil;

/**
 * Created by zhenhong.gzh on 15/8/17.
 *
 * CNNFeatureTrain 算法的日志格式化输出
 */
public class CNNDetailProgressHelper extends XFlowProgressHelper {

  private static int interval = 30000;

  static {
    try {
      Properties properties = PluginUtil.getPluginProperty(PAICommand.class);
      String intervalString = properties.getProperty("cnn_progress_interval", "30000");
      interval = Integer.parseInt(intervalString);
    } catch (IOException e) {
      //do nothing
    }

  }

  public static final String ALGO_NAME = "CNN_FEATURE_TRAIN";

  CNNDetailProgressHelper() {
    super.setInterval(interval);
  }

  @Override
  public String getPAIAlgoName() {
    return ALGO_NAME;
  }


  boolean isTerminated = false;

  @Override
  public boolean needProgressMessage(Instance instance) {
    //FOR CNN ALGO
    //we should print one more progress message after instance termianted.
    boolean result = isTerminated;
    if (instance.isTerminated()) {
      isTerminated = true;
    }
    return !result;
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
      String details = instance.getTaskDetailJson2(taskName);

      if (StringUtils.isNullOrEmpty(details)) {
        writer.print(taskName + ": running");
      } else {
        String formatStr = getDetailFormattedString(details);
        if (StringUtils.isNullOrEmpty(formatStr)) {
          formatStr = "running...";
        }
        writer.print(taskName + " : \n");

        writer.print(formatStr);
      }
      if (++i < taskNames.size()) {
        writer.print("\n");
      }
    }

    String str = strWriter.toString();
    sb.append(str);
    return sb.toString();
  }

  public String getDetailFormattedString(String details) throws ODPSConsoleException {
    try {
      JSONArray messages = JSON.parseObject(details).getJSONArray("message");

      StringBuilder result = new StringBuilder();

      for (int idx = 0; idx < messages.size(); ++idx) {
        JSONObject stage = messages.getJSONObject(idx);
        result.append(String.format("%s\t\t%s\n", stage.getString("time"), stage.getString("msg")));
      }

      return result.toString();

    } catch (Exception e) {
      return null;
    }
  }
}

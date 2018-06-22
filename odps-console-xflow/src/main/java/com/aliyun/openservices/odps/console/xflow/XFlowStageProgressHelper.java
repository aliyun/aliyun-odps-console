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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by zhenhong on 15/8/20.
 */
public class XFlowStageProgressHelper extends XFlowProgressHelper {

  public String getPAIAlgoName() {
    return "";
  }

  XFlowStageProgressHelper() {
    super.setInterval(5000);
  }

  @Override
  public String getProgressMessage(Instance instance)
      throws ODPSConsoleException, OdpsException {
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

    String str = strWriter.toString();
    sb.append(str);
    return sb.toString();
  }

  @Override
  public boolean needProgressMessage(Instance instance) throws ODPSConsoleException, OdpsException {
    return !instance.isTerminated();
  }

}

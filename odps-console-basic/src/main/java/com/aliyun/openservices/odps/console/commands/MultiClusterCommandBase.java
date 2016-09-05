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

package com.aliyun.openservices.odps.console.commands;

import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.Task;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;

public abstract class MultiClusterCommandBase extends AbstractCommand {

  protected String instanceId = "";

  public MultiClusterCommandBase(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  protected void runJob(Task task) throws OdpsException, ODPSConsoleException {

    ExecutionContext context = getContext();

    Map<String, String> config = task.getProperties();
    addSetting(config, SetCommand.setMap);
    for (Entry<String, String> property : config.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }

    InstanceRunner runner = new InstanceRunner(getCurrentOdps(), task, context);
    runner.submit();

    //delay hooks after print result
    OdpsHooks hooks = runner.getInstance().getOdpsHooks();
    runner.getInstance().setOdpsHooks(null);

    if (context.isAsyncMode()) {
      instanceId = runner.getInstance().getId();
      // 如果是异常模式,提交job后直接退出
      return;
    }

    String queryResult = runner.waitForCompletion();
    // 执行完了再重新拿一次
    instanceId = runner.getInstance().getId();

    if (queryResult != null && !queryResult.trim().equals("")) {
      writeResult(queryResult);
    }

    hooks.after(runner.getInstance(), getCurrentOdps());
  }

  protected void writeResult(String queryResult) throws OdpsException, ODPSConsoleException {
    DefaultOutputWriter writer = getContext().getOutputWriter();
    writer.writeResult(queryResult);
  }

  protected static void addSetting(Map<String, String> config, Map<String, String> setting) {
    String origSettings = null;
    String addedSettings = null;

    Entry<String, String> property = null;
    for (Entry<String, String> pr : config.entrySet()) {
      if (pr.getKey().equals("settings")) {
        property = pr;
        origSettings = pr.getValue();
        break;
      }
    }
    if (property == null || StringUtils.isNullOrEmpty(origSettings)) {
      try {
        JSONObject js = new JSONObject(setting);
        if(!js.toString().equals("{\"empty\":false}")){
          addedSettings = js.toString();
        }
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        config.put("settings", addedSettings);
      } else {
        return;
      }
    } else {
      try {
        JSONObject json = new JSONObject(origSettings);
        for (Entry<String, String> prop : setting.entrySet()) {
          json.put(prop.getKey(), prop.getValue());
        }
        addedSettings = json.toString();
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        property.setValue(addedSettings);
      } else {
        return;
      }
    }
  }

}

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

package com.aliyun.openservices.odps.console.commands.logview;

import java.util.LinkedHashMap;
import java.util.Set;

public class ActionRegistry {
  private static LinkedHashMap<String, Class<? extends LogViewBaseAction>> actionsMap = new LinkedHashMap<String, Class<? extends LogViewBaseAction>>();
  static {
    actionsMap.put(GetStatusAction.ACTION_NAME, GetStatusAction.class);
    actionsMap.put(GetTaskDetailsAction.ACTION_NAME, GetTaskDetailsAction.class);
    actionsMap.put(GetLogAction.ACTION_NAME, GetLogAction.class);
    actionsMap.put(HelpAction.ACTION_NAME, HelpAction.class);
  }

  public static LogViewBaseAction getAction(String name, LogViewContext ctx) {
    Class<? extends LogViewBaseAction> clz = actionsMap.get(name.toLowerCase());
    if (clz == null) {
      return null;
    }
    try {
      LogViewBaseAction action = clz.newInstance();
      action.ctx = ctx;
      return action;
    } catch (InstantiationException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
  }

  public static HelpAction getHelpAction(LogViewContext ctx, String msg) {
    HelpAction action = (HelpAction) getAction(HelpAction.ACTION_NAME, ctx);
    action.setAuxiliaryMessage(msg);
    return action;
  }

  public static Set<String> getActionNames() {
    return actionsMap.keySet();
  }
}

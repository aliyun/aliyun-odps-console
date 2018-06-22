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

import java.util.HashMap;

/**
 * Created by zhenhong on 15/8/18.
 */
public class XFlowProgressHelperRegistry {
  private static HashMap<String, Class<? extends XFlowProgressHelper>>
      actionsMap = new HashMap<String, Class<? extends XFlowProgressHelper>>();
  static {
    actionsMap.put(CNNDetailProgressHelper.ALGO_NAME, CNNDetailProgressHelper.class);
  }

  public static XFlowProgressHelper getProgressHelper(String algoName) {
    if (!actionsMap.containsKey(algoName.toUpperCase())) {
      return new XFlowStageProgressHelper();
    }

    Class<? extends XFlowProgressHelper> clz = actionsMap.get(algoName.toUpperCase());
    if (clz == null) {
      return null;
    }
    try {
      XFlowProgressHelper fetcher = clz.newInstance();
      return fetcher;
    } catch (InstantiationException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
  }

}

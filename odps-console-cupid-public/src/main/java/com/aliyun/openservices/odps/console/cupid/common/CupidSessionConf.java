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

package com.aliyun.openservices.odps.console.cupid.common;

import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.commands.SetCommand;


public final class CupidSessionConf {

  /**
   * get 4 basic cupid configurations
   *
   * @param context
   *     Command context
   * @return
   */
  public static CupidConf getBasicCupidConf(ExecutionContext context) {
    CupidConf cupidConf = new CupidConf();
    if (SetCommand.setMap.containsKey(CupidConstants.ODPS_TASK_MAJOR_VERSION)) {
      cupidConf.set(CupidConstants.ODPS_TASK_MAJOR_VERSION, SetCommand.setMap.get(CupidConstants.ODPS_TASK_MAJOR_VERSION));
    } else {
      cupidConf.set(CupidConstants.ODPS_TASK_MAJOR_VERSION, CupidConstants.ODPS_TASK_MAJOR_VERSION_DEFAULT);
    }
    if (SetCommand.setMap.containsKey(CupidConstants.ODPS_CUPID_CONTAINER_NODE_LABEL)) {
      cupidConf.set(CupidConstants.ODPS_CUPID_CONTAINER_NODE_LABEL, SetCommand.setMap.get(CupidConstants.ODPS_CUPID_CONTAINER_NODE_LABEL));
    } else {
      cupidConf.set(CupidConstants.ODPS_CUPID_CONTAINER_NODE_LABEL, CupidConstants.ODPS_CUPID_CONTAINER_NODE_LABEL_DEFAULT);
    }

    String defaultTrackurlHost = context.getEndpoint() != null && context.getEndpoint().toLowerCase().contains("maxcompute.aliyun") ?
        "http://jobview.odps.aliyun.com" :
        "http://jobview.odps.aliyun-inc.com";
    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_MOYE_TRACKURL_HOST, defaultTrackurlHost);
    if (SetCommand.setMap.containsKey(CupidConstants.CUPID_CONF_ODPS_MOYE_TRACKURL_HOST)) {
      cupidConf.set(CupidConstants.CUPID_CONF_ODPS_MOYE_TRACKURL_HOST,
          SetCommand.setMap.get(CupidConstants.CUPID_CONF_ODPS_MOYE_TRACKURL_HOST));
    }

    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT, context.getEndpoint().replace("aliyun.com", "aliyun-inc.com"));
    if (SetCommand.setMap.containsKey(CupidConstants.CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT)) {
      cupidConf.set(CupidConstants.CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT,
          SetCommand.setMap.get(CupidConstants.CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT));
    }

    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_ACCESS_ID, context.getAccessId());
    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_ACCESS_KEY, context.getAccessKey());
    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_END_POINT, context.getEndpoint());
    cupidConf.set(CupidConstants.CUPID_CONF_ODPS_PROJECT_NAME, context.getProjectName());

    return cupidConf;
  }
}

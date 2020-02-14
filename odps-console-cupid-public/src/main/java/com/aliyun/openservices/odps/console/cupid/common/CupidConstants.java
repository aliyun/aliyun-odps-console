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

import com.aliyun.openservices.odps.console.ExecutionContext;

public final class CupidConstants {

  // Conf To set cupid session
  public final static String CUPID_CONF_ODPS_ACCESS_ID = "odps.access.id";
  public final static String CUPID_CONF_ODPS_ACCESS_KEY = "odps.access.key";
  public final static String CUPID_CONF_ODPS_END_POINT = "odps.end.point";
  public final static String CUPID_CONF_ODPS_PROJECT_NAME = "odps.project.name";
  public final static String CUPID_CONF_ODPS_MOYE_TRACKURL_HOST = "odps.moye.trackurl.host";
  public final static String CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT =
      "odps.cupid.webproxy.endpoint";

  // To get proxy token
  public final static String DEFAULT_EXTENSIVE_DOMAIN_NAME = "cupid.aliyun-inc.com";
  public final static int DEFAULT_EXPIRED_HOURS = 24;

  // Service type
  public final static String SERVICE_TYPE_WEBCONSOLE = "webconsole";
  public final static String SERVICE_TYPE_SPARKSHELL = "sparkshell";

  // Pod label of service type
  public final static String Pod_Label_SERVICE_TYPE = "serviceType";

  // Pod definition yaml file path
  public final static String WEB_CONSOLE_POD_DEF_PATH = "/webConsolePodDefinition.yaml";

  // K8s cluster config
  public final static String ODPS_TASK_MAJOR_VERSION = "odps.task.major.version";
  public final static String ODPS_TASK_MAJOR_VERSION_DEFAULT = "cupid_v2";
  public final static String ODPS_CUPID_CONTAINER_NODE_LABEL= "odps.cupid.container.node.label";
  public final static String ODPS_CUPID_CONTAINER_NODE_LABEL_DEFAULT = "system:alios7";
  public final static String ODPS_CUPID_KUBE_MASTER_MODE = "odps.cupid.kube.master.mode";
  public final static String ODPS_CUPID_KUBE_MASTER_MODE_DEFAULT = "cupid";
}
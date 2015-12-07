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

package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;

/**
 * @author shuman.gansm
 */
public class AuthorizationConsoleUtil {

  public static String generateToken(Odps odps, String url) throws OdpsException {

    // 改为7天
    int day = 7 * 60 * 60 * 24;

    long expires = System.currentTimeMillis() / 1000 + day;
    String
        policy =
        " {\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"odps:Read\",\"Resource\":\"acs:odps:*:"
        + url.toLowerCase() + "\"}]}";

    return odps.projects().get().getSecurityManager().generateAuthorizationToken(policy, "BEARER");
  }

}

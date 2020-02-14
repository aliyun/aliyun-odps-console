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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.openservices.odps.console.ExecutionContext;

public class BearerTokenGenerator {
    public static String getBearerToken(ExecutionContext cxt) throws OdpsException {
        AliyunAccount account = new AliyunAccount(cxt.getAccessId(), cxt.getAccessKey());
        Odps odps = new Odps(account);
        odps.setEndpoint(cxt.getEndpoint());
        odps.setDefaultProject(cxt.getProjectName());
        // expires in 7 days
        String policy = "{\n" +
                "\"expires_in_hours\": 168,\n" +
                "\"policy\": {\n" +
                "\"Statement\": [\n" +
                "{\n" +
                "\"Action\": [\"odps:*\"],\n" +
                "\"Effect\": \"Allow\",\n" +
                "\"Resource\": \"acs:odps:*:*\"\n" +
                "}\n" +
                "],\n" +
                "\"Version\": \"1\"\n" +
                "}\n" +
                "}\n";
        return odps.projects().get().getSecurityManager().generateAuthorizationToken(policy, "bearer");
    }
}

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

import apsara.odps.cupid.protocol.CupidTaskParamProtos;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.requestcupid.ApplicationMetaUtil;
import com.aliyun.odps.cupid.requestcupid.JobViewUtil;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
// import org.apache.log4j.Level;

public class SparkJobUtils
{
    public static String jobViewService(ExecutionContext executionContext,
                                        String instanceId,
                                        String applicationId) throws ODPSConsoleException
    {
        CupidConf conf = CupidSessionConf.getBasicCupidConf(executionContext);
        CupidSession session = new CupidSession(conf);

        CupidTaskParamProtos.ApplicationMeta applicationMeta = getApplicationMeta(instanceId, applicationId, session);
        // Level originLevel = org.apache.log4j.Logger.getRootLogger().getLevel();
        // org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);

        String jobViewUrl;
        JobViewUtil util = new JobViewUtil(session);
        jobViewUrl = util.generateJobView(applicationMeta.getInstanceId());

        // org.apache.log4j.Logger.getRootLogger().setLevel(originLevel);
        return jobViewUrl;
    }

    public static String logViewService(ExecutionContext ec, String instanceId, String applicationId) throws ODPSConsoleException, OdpsException {
        CupidConf conf = CupidSessionConf.getBasicCupidConf(ec);
        CupidSession session = new CupidSession(conf);

        String logViewHost = session.odps().logview().getLogViewHost();
        String odpsEndpoint = session.odps().getEndpoint();
        String bearerToken = BearerTokenGenerator.getBearerToken(ec);

        Odps odps = OdpsConnectionFactory.createOdps(ec);

        CupidTaskParamProtos.ApplicationMeta applicationMeta = getApplicationMeta(instanceId, applicationId, session);
        instanceId = applicationMeta.getInstanceId();
        return logViewHost +
            "/logview/?h=" + odpsEndpoint +
            "&p=" + odps.getDefaultProject() +
            "&i=" + instanceId +
            "&token=" + bearerToken;
    }

    public static CupidTaskParamProtos.ApplicationMetaList getApplicationMetaList(CupidSession session)
        throws ODPSConsoleException
    {
        return getApplicationMetaList(session, null);
    }

    public static CupidTaskParamProtos.ApplicationMetaList getApplicationMetaList(CupidSession session, String yarnApplicationState)
        throws ODPSConsoleException
    {
        CupidTaskParamProtos.ApplicationMetaList applicationMetaList;
        // Level originLevel = org.apache.log4j.Logger.getRootLogger().getLevel();
        // org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);
        try {
            applicationMetaList =
                ApplicationMetaUtil.listApplicationMeta("SPARK", yarnApplicationState, session);
        } catch (Exception e) {
            throw new ODPSConsoleException("Get Application Meta List failed.", e);
        }

        // org.apache.log4j.Logger.getRootLogger().setLevel(originLevel);
        return applicationMetaList;
    }

    public static CupidTaskParamProtos.ApplicationMeta getApplicationMeta(
        String instanceId,
        String applicationId,
        CupidSession session
    ) throws ODPSConsoleException
    {
        CupidTaskParamProtos.ApplicationMeta applicationMeta = null;
        // Level originLevel = org.apache.log4j.Logger.getRootLogger().getLevel();
        // org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);

        // switch log level to off to prevent trace log from cupid-sdk
        // get application meta via instanceId or applicationId
        if (instanceId != null) {
            try {
                applicationMeta = ApplicationMetaUtil.getCupidInstanceMeta(instanceId, session);
            } catch (Exception e) {
                throw new ODPSConsoleException("Getting Cupid Instance Meta failed.", e);
            }
        } else if (applicationId != null) {
            try {
                applicationMeta = ApplicationMetaUtil.getApplicationMeta(applicationId, session);
            } catch (Exception e) {
                throw new ODPSConsoleException("Getting Application Meta failed.", e);
            }
        }

        if (applicationMeta == null) {
            throw new ODPSConsoleException("Getting Meta failed.");
        }

        // org.apache.log4j.Logger.getRootLogger().setLevel(originLevel);
        return applicationMeta;
    }

    public static void updateApplicationMeta(String applicationId,
                                             CupidSession session,
                                             CupidTaskParamProtos.ApplicationMeta meta) throws ODPSConsoleException
    {
        // Level originLevel = org.apache.log4j.Logger.getRootLogger().getLevel();
        // org.apache.log4j.Logger.getRootLogger().setLevel(Level.OFF);

        try {
            ApplicationMetaUtil.updateApplicationMeta(applicationId, meta, session);
        } catch (Exception e) {
            throw new ODPSConsoleException("update Application Meta failed.", e);
        }

        // org.apache.log4j.Logger.getRootLogger().setLevel(originLevel);
    }
}

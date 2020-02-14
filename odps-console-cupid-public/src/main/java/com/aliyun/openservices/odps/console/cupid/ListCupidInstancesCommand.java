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

package com.aliyun.openservices.odps.console.cupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.requestcupid.ApplicationMetaUtil;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.cupid.common.CupidSessionConf;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import java.io.PrintStream;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ListCupidInstancesCommand extends AbstractCommand {

  private CupidSession cupidSession = null;

  private enum AppState {
    NEW,
    NEW_SAVING,
    SUBMITTED,
    ACCEPTED,
    RUNNING,
    FINISHED,
    FAILED,
    KILLED
  }

  public ListCupidInstancesCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    CupidConf cupidConf = CupidSessionConf.getBasicCupidConf(getContext());
    cupidSession = new CupidSession(cupidConf);
  }

  public static final String[] HELP_TAGS = new String[]{"cupid", "list", "ls",  "instance", "instances"};

  private String getAppStateStr(long order) throws ODPSConsoleException {
    AppState[] states = AppState.values();
    if (order >= states.length || order < 0) {
      throw new ODPSConsoleException("Cupid application state illegal: " + order);
    }
    return states[(int) order].name();
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    CupidTaskParamProtos.ApplicationMetaList applicationMetaList = null;
    try {
      applicationMetaList =
              ApplicationMetaUtil.listApplicationMeta(null, null, cupidSession);
    } catch (Exception ex) {
      throw new ODPSConsoleException(ex);
    }
    String [] title = {"StartTime", "RunTime", "Status", "InstanceID"};
    // 设置每一列的百分比
    int [] columnPercent = {15, 6, 6, 20};
    ODPSConsoleUtils.formaterTableRow(title, columnPercent, getContext().getConsoleWidth());
    String[] attr = new String[4];
    for (CupidTaskParamProtos.ApplicationMeta applicationMeta : applicationMetaList.getApplicationMetaListList()) {
      String instanceId = applicationMeta.getInstanceId();
      attr[3] = instanceId;
      long intState = applicationMeta.getYarnApplicationState();
      String sState = getAppStateStr(intState);
      attr[2] = sState;
      long sTime = applicationMeta.getStartedTime();
      Date date = new Date(sTime);
      attr[0] = ODPSConsoleUtils.formatDate(date);
      long eTime = applicationMeta.getFinishedTime();
      long rTime = 0;
      if (intState == AppState.FAILED.ordinal() || intState == AppState.FAILED.ordinal() ||
              intState == AppState.KILLED.ordinal()) {
        rTime = (eTime - sTime);
      } else {
        rTime = System.currentTimeMillis() - sTime;
      }
      attr[1] = (rTime / 1000) + "s";
      ODPSConsoleUtils.formaterTableRow(attr, columnPercent, getContext().getConsoleWidth());
    }
  }

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: list|ls cupidinstances");
    stream.println();
  }

  public static ListCupidInstancesCommand parse(String cmd, ExecutionContext cxt)
          throws ODPSConsoleException {

    String regstr = "\\s*(list|ls)\\s+cupidinstances([\\s\\S]*)";
    Pattern pattern = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(cmd);

    if (matcher.matches()) {
      return new ListCupidInstancesCommand(cmd, cxt);
    }

    return null;
  }

}

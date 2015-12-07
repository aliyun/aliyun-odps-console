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

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public abstract class LogViewBaseAction {
  protected LogViewContext ctx;

  public LogViewContext getCtx() {
    return ctx;
  }

  public void setCtx(LogViewContext ctx) {
    this.ctx = ctx;
  }

  protected LogViewBaseAction() {
  }

  public abstract void run() throws OdpsException, ODPSConsoleException;

  public abstract String getActionName();

  public void parse(String[] args) throws LogViewArgumentException {
    Options options = getOptions();
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine cl = parser.parse(options, args);
      configure(cl);
    } catch (ParseException e) {
      throw new LogViewArgumentException(e.getMessage());
    }
  }

  protected void configure(CommandLine cl) throws LogViewArgumentException {
    if (cl.getArgs().length > 0) {
      throw new LogViewArgumentException("Unexpected argument: " + cl.getArgs()[0]);
    }
  }

  protected Options getOptions() {
    Options options = new Options();
    return options;
  }

  protected DefaultOutputWriter getWriter() {
    return ctx.getSession().getOutputWriter();
  }

  protected String getHelpPrefix() {
    return "log " + getActionName();
  }

  protected static String deduceTaskName(Instance inst) throws ODPSConsoleException, OdpsException {
    Map<String, TaskStatus> ss = inst.getTaskStatus();
    if (ss.size() == 1) {
      for (String key : ss.keySet()) {
        return key;
      }
    } else {
      throw new ODPSConsoleException("Please specify one of these tasks with option '-t': "
          + StringUtils.join(ss.keySet(), ','));
    }
    return null;
  }
}

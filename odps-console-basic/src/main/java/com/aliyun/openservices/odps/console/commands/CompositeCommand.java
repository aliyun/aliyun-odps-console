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

import java.util.List;

import org.jsoup.nodes.Document;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * @author shuman.gansm
 * **/
public class CompositeCommand extends AbstractCommand {

  private List<AbstractCommand> actionList = null;

  public List<AbstractCommand> getActionList() {
    return actionList;
  }

  public CompositeCommand(List<AbstractCommand> list, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.actionList = list;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    for (AbstractCommand action : actionList) {
      try {
        action.run();
      } catch (Exception e) {

        if (e instanceof ODPSConsoleException) {
          // 如果抛出的是ODPSConsoleException，有可能会设置exitCode直接抛出
          throw (ODPSConsoleException) e;
        }

        // 其它的作为cause传进去
        throw new ODPSConsoleException(e.getMessage(), e);
      }

    }
  }

  @Override
  public String runHtml(Document dom) throws OdpsException, ODPSConsoleException {
    StringBuilder sb = new StringBuilder();
    for (AbstractCommand action : actionList) {
      sb.append(action.runHtml(dom));
    }
    return sb.toString();
  }

}

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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class HtmlModeCommand extends AbstractCommand {

  public HtmlModeCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {
    getContext().setHtmlMode(true);
    URL url = this.getClass().getClassLoader().getResource("html");
    if (url == null) {
      throw new ODPSConsoleException("Html folder not exists in classpath.");
    }

    File source = new File(url.getFile());
    File dest = new File("html");
    if (dest.exists()) {
      System.err.println(dest + " exists.");
      return;
    }

    try {
      FileUtils.copyDirectory(source ,dest);
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    }

  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static HtmlModeCommand parse(List<String> optionList, ExecutionContext sessionContext) {

    if (optionList.contains("--html")) {

      optionList.remove(optionList.indexOf("--html"));
      return new HtmlModeCommand("--html", sessionContext);
    }
    return null;
  }
}

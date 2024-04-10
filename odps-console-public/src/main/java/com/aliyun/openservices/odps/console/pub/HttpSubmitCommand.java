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

package com.aliyun.openservices.odps.console.pub;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.DirectCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ExtProperties;

/**
 * command: http <method> <url> [reqfile];
 * 
 * @author shuman.gansm
 * */
public class HttpSubmitCommand extends DirectCommand {

  private enum HttpMethod {
    DELETE,
    GET,
    HEAD,
    POST,
    PUT
  }

  public static final String[] HELP_TAGS = new String[]{"http"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: http <method> <url> [-header=file] [-content=file] [-token=logview_token]");
  }

  String fileName;
  HttpMethod method;
  String url;
  String headerFileName;
  String token;

  Map<String, String> parameters;

  public String getFileName() {
    return fileName;
  }

  public String getHeaderFileName() {
    return headerFileName;
  }

  public HttpMethod getMethod() {
    return method;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public HttpSubmitCommand(String commandText, ExecutionContext context, HttpMethod method, String url) {
    super(commandText, context);
    this.method = method;
    this.url = url;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option header_file = new Option("h", "header", true, "header file name");
    header_file.setRequired(false);

    Option content_file = new Option("c", "content", true, "content file name");
    content_file.setRequired(false);

    Option token = new Option("t", "token", true, "logview token");
    content_file.setRequired(false);

    opts.addOption(header_file);
    opts.addOption(content_file);
    opts.addOption(token);

    return opts;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    RestClient client;
    if (!StringUtils.isNullOrEmpty(token)) {
      client = new RestClient(new DefaultTransport());
      client.setEndpoint(getContext().getEndpoint());
      client.setAccount(new BearerTokenAccount(token));
    } else {
      client = getCurrentOdps().getRestClient();
    }

    InputStream content = null;
    long contentLength = 0;
    Map<String, String> headers = null;

    if (getHeaderFileName() != null) {
      headers = new HashMap<String, String>();

      File file = new File(getHeaderFileName());
      if (!file.exists()) {
        throw new ODPSConsoleException("file not exist.");
      }

      FileInputStream fileInput = null;
      try {
        fileInput = new FileInputStream(file);
        Properties properties = new ExtProperties();
        properties.load(fileInput);

        Enumeration enuKeys = properties.keys();
        while(enuKeys.hasMoreElements()) {
          String key = (String)enuKeys.nextElement();
          String value = properties.getProperty(key);

          if (!StringUtils.isNullOrEmpty(key) && !StringUtils.isNullOrEmpty(value)) {
            headers.put(key, value);
          }
        }
      } catch (IOException e) {
        throw new ODPSConsoleException("can not read file.");
      } finally {
        IOUtils.closeQuietly(fileInput);
      }
    }

    if (getFileName() != null) {

      File file = new File(getFileName());
      if (!file.exists()) {
        throw new ODPSConsoleException("file not exist.");
      }

      contentLength = file.length();
      try {
        content = new FileInputStream(file);
        // System.out.println(FileUtil.readFromStream(content));
      } catch (Exception e) {
        throw new ODPSConsoleException("can not read file.");
      }
    }

    try {
      Response response =
          client.request(url, method.toString(), parameters, headers, content, contentLength);

      // c out response
      try {
        Map<String, String> resHeaders = response.getHeaders();
        for (String key : resHeaders.keySet()) {
          String value = resHeaders.get(key);
          System.out.println(key + ": " + value);

        }
        // 添加报文的换行
        System.out.println("");
        System.out.println(new String(response.getBody(), "utf-8"));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    } finally {
      IOUtils.closeQuietly(content);
    }
  }

  public static HttpSubmitCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    commandString = commandString.trim();
    if (commandString.toUpperCase().matches("HTTP[\\s\\S]*")) {

      String[] options = commandString.split("\\s+");

      if (options.length < 3 || options.length > 6) {
        throw new ODPSConsoleException("illegal parameter");
      }

      HttpMethod method = null;
      try {
        method = HttpMethod.valueOf(options[1].toUpperCase());
      } catch (Exception e) {
        throw new ODPSConsoleException("illegal http method");
      }

      String[] splits = options[2].split("\\?", 2);
      String url = splits[0];

      Map<String, String> parameters = new HashMap<String, String>();
      if (splits.length == 2) {

        String[] paras = splits[1].split("&");

        for (int i = 0; i < paras.length; i++) {
          String[] keyValue = paras[i].split("=", 2);

          // 可能value是没有值的,如 progress&taskname=sql
          parameters.put(keyValue[0], keyValue.length == 1 ? null : keyValue[1]);
        }
      }

      HttpSubmitCommand command = new HttpSubmitCommand(commandString, sessionContext, method, url);
      command.parameters = parameters;

      if (options.length > 3) {
        Options opts = initOptions();
        CommandLineParser clp = new GnuParser();
        CommandLine cl;
        try {
          cl = clp.parse(opts, options, false);
        } catch (Exception e) {
          throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
        }

        if (cl.hasOption("header")) {
          command.headerFileName = cl.getOptionValue("header");
        }

        if (cl.hasOption("content")) {
          command.fileName = cl.getOptionValue("content");
        }

        if (cl.hasOption("token")) {
          command.token = cl.getOptionValue("token");
        }

        if (cl.getArgs().length > 3) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "too much parameters");
        }
      }

      return command;
    }

    return null;
  }
}

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

package com.aliyun.openservices.odps.console.mapreduce.runtime;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.openservices.odps.console.utils.CommandExecutor;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource.Type;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.CommandExecutor.ExecutorResult;
import com.aliyun.openservices.odps.console.mr.MapReduceCommand;

import org.jline.reader.UserInterruptException;

public class MapReduceJob implements MapReduceJobLauncher {

  private ExecutionContext context;
  private MapReduceCommand mrCmd;
  private Odps odps;

  public MapReduceJob(MapReduceCommand mrCmd) throws ODPSConsoleException {
    this.context = mrCmd.getContext();
    this.mrCmd = mrCmd;
    this.odps = mrCmd.getCurrentOdps();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.aliyun.openservices.odps.console.mapreduce.runtime.MapReduceJobLauncher
   * #run()
   */
  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    String prjName = context.getProjectName();
    if (prjName == null || prjName.trim().equals("")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.PROJECT_NOT_BE_SET);
    }

    addTempResources();

    makeAlias4TempResources();

    try {
      runMRTask();
    } finally {
      makeUnalias4TempResources();
      dropTempResources();
    }
  }

  private void addTempResources() throws ODPSConsoleException, OdpsException {

    Map<String, List<String>> tempResources = mrCmd.getTempResources();
    if (!tempResources.isEmpty()) {
      for (Map.Entry<String, List<String>> entry : tempResources.entrySet()) {
        String resAlias = entry.getKey();
        List<String> resInfo = entry.getValue();
        Type type = Type.valueOf(resInfo.get(0).toUpperCase());
        FileResource resource = null;
        switch (type) {
          case PY:
            resource = new PyResource();
            break;
          case JAR:
            resource = new JarResource();
            break;
          case ARCHIVE:
            resource = new ArchiveResource();
            break;
          case FILE:
            resource = new FileResource();
            break;
          default:
            throw new ODPSConsoleException("unsupported resource type: " + type);
        }

        resource.setIsTempResource(true);

        // append uuid to file name
        String resName = UUID.randomUUID().toString() + "_" + resAlias;
        resource.setName(resName);
        // upload resource
        File resFile = new File(entry.getValue().get(1));
        FileInputStream inputStream = null;
        try {
          inputStream = new FileInputStream(resFile);
          odps.resources().create(resource, inputStream);
        } catch (IOException ex) {
          throw new ODPSConsoleException(ODPSConsoleConstants.FILE_UPLOAD_FAIL, ex);
        } finally {
          if (inputStream != null) {
            try {
              inputStream.close();
            } catch (IOException ex) {

            }
          }
        }

        // replace file path using resource name, for alias and drop
        resInfo.set(1, resName);
      }
    }
  }

  private void makeAlias4TempResources() {
    Map<String, List<String>> tempResources = mrCmd.getTempResources();
    if (!tempResources.isEmpty()) {
      for (Map.Entry<String, List<String>> entry : tempResources.entrySet()) {
        SetCommand.aliasMap.put(entry.getKey(), entry.getValue().get(1));
      }
    }
  }

  private void makeUnalias4TempResources() {
    Map<String, List<String>> tempResources = mrCmd.getTempResources();
    if (!tempResources.isEmpty()) {
      for (Map.Entry<String, List<String>> entry : tempResources.entrySet()) {
        SetCommand.aliasMap.remove(entry.getKey());
      }
    }
  }

  private void dropTempResources() {
    Map<String, List<String>> tempResources = mrCmd.getTempResources();
    if (!tempResources.isEmpty()) {
      for (Map.Entry<String, List<String>> entry : tempResources.entrySet()) {
        try {
          odps.resources().delete(entry.getValue().get(1));
        } catch (Exception ex) {
          // Ignore exception
        }
      }
    }
  }

  private void runMRTask() throws OdpsException, ODPSConsoleException {
    StringBuffer cmd = new StringBuffer("java");

    // cmd.append(" -Dodps.access.id=").append(str(context.getAccessId()));
    // cmd.append(" -Dodps.access.key=").append(str(context.getAccessKey()));
    // cmd.append(" -Dodps.refresh.token=").append(str(context.getRefreshToken()));
    cmd.append(" -Dodps.project.name=").append(str(context.getProjectName()));
    cmd.append(" -Dodps.end.point=").append(str(context.getEndpoint()));

    if (context.getLogViewHost() != null) {
      cmd.append(" -Dodps.logview.host=").append(str(context.getLogViewHost()));
    }
    
    if (mrCmd.getResources() != null && !mrCmd.getResources().trim().equals("")) {
      cmd.append(" -Dodps.cache.resources=" + mrCmd.getResources());
    }
    if (mrCmd.getLibjars() != null && !mrCmd.getLibjars().trim().equals("")) {
      cmd.append(" -Dodps.classpath.resources=" + mrCmd.getLibjars());
    }
    if (mrCmd.getJvmOptions() != null) {
      for (String jvmOpt : mrCmd.getJvmOptions()) {
        cmd.append(" " + jvmOpt);
      }
    }
    if (mrCmd.isLocalMode()) {
      cmd.append(" -Dodps.runner.mode=local");
    }
    if (mrCmd.isCostMode()) {
      cmd.append(" -Dcost=true");
    }
    if (!mrCmd.getConf().isEmpty()) {
      cmd.append(" -Dodps.mr.job.conf=" + mrCmd.getConf());
    }

    switch (context.getAccountProvider()) {
      case ALIYUN:
        cmd.append(" -Dodps.account.provider=aliyun");
        break;
      case STS:
        cmd.append(" -Dodps.account.provider=sts");
        cmd.append(" -Dodps.sts.token=").append(str(context.getStsToken()));
        break;
      default:
        throw new ODPSConsoleException(ODPSConsoleConstants.UNSUPPORTED_ACCOUNT_PROVIDER);
    }

    cmd.append(" -Dodps.access.id=").append(str(context.getAccessId()));
    cmd.append(" -Dodps.access.key=").append(str(context.getAccessKey()));
    cmd.append(" -Dodps.app.access.id=").append(str(context.getAppAccessId()));
    cmd.append(" -Dodps.app.access.key=").append(str(context.getAppAccessKey()));

    cmd.append(" -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog");
    cmd.append(" -Dorg.apache.commons.logging.simplelog.log.org.apache.http=WARN");

    String fileName = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator")
        + "tmp_mr_" + System.currentTimeMillis() + "_" + getPID() + ".json";

    if (writeConfig(fileName)) {
      cmd.append(" -Dodps.exec.context.file=").append(fileName);
    }

    String sep = System.getProperty("path.separator");

    String classpath = "";
    String clt_classpath = System.getProperty("java.class.path");
    if (!StringUtils.isEmpty(clt_classpath)) {
      classpath += (classpath.isEmpty() ? "" : sep) + clt_classpath;
    }
    if (!StringUtils.isEmpty(mrCmd.getClasspath())) {
      classpath += (classpath.isEmpty() ? "" : sep) + mrCmd.getClasspath();
    }
    if (!classpath.isEmpty()) {
      cmd.append(" -classpath " + classpath);
    }

    if (mrCmd.getArgs() != null) {
      cmd.append(" ").append(mrCmd.getArgs());
    }



    try {
      ExecutorResult result = CommandExecutor.run(parseCmd(cmd.toString()), true);
      if (result.getEcode() != 0) {
        throw new ODPSConsoleException("Run job failed.", null, result.getEcode());
      }
    } catch (IOException io) {
      throw new OdpsException("CommandExecutor read stream failed:" + io.getMessage());
    } catch (UserInterruptException ex) {
      throw ex;
    } finally {
      new File(fileName).delete();
    }

  }

  private String[] parseCmd(String cmd) throws IOException {
    cmd = cmd.trim();
    if (StringUtils.isEmpty(cmd)) {
      throw new IOException("cmd is empty, because main class not specified.");
    }

    List<String> result = new ArrayList<String>();
    String[] ss = StringUtils.splitPreserveAllTokens(cmd);

    int idx = 0;
    boolean quot = false;
    StringBuilder buffer = new StringBuilder();
    while (idx < ss.length) {
      if (quot) {
        buffer.append(' ');
      }

      // skip empty tokens;
      while (idx < ss.length && ss[idx].isEmpty()) {
        if (quot) {
          buffer.append(' ');
        }
        idx++;
      }

      if (idx >= ss.length) {
        break;
      }

      String tok = ss[idx];

      if (tok.startsWith("\"") || tok.endsWith("\"")) {
        if (tok.startsWith("\"")) {
          if (quot) {
            buffer.append(tok);
          } else {
            buffer.append(tok.substring(1));
          }
          quot = true;
        }

        if (tok.endsWith("\"") && tok.length() > 1) {
          quot = false;
          if (tok.startsWith("\"")) {
            buffer.deleteCharAt(buffer.length() - 1);
          } else {
            buffer.append(tok.substring(0, tok.length() - 1));
          }
          result.add(buffer.toString());
          buffer = new StringBuilder();
        }
      } else {
        if (quot) {
          buffer.append(tok);
        } else {
          result.add(tok);
        }
      }

      idx++;
    }

    if (quot) {
      throw new IOException("exist unmatched quotations: " + cmd);
    }
    return result.toArray(new String[result.size()]);
  }

  private String str(String s) {
    return (s == null) ? "" : s;
  }

  private boolean writeConfig(String fileName) throws OdpsException {
    DataOutputStream out = null;
    try {
      out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
          new File(fileName))));

      JsonObject result = new JsonObject();

      if (!SetCommand.setMap.isEmpty()) {
        String setMapJson = new GsonBuilder().disableHtmlEscaping().create().toJson(SetCommand.setMap);
        result.add("settings", new JsonParser().parse(setMapJson).getAsJsonObject());
      }

      if (!SetCommand.aliasMap.isEmpty()) {
        String aliasMapJson = new GsonBuilder().disableHtmlEscaping().create().toJson(SetCommand.aliasMap);
        result.add("aliases", new JsonParser().parse(aliasMapJson).getAsJsonObject());
      }

      result.addProperty("commandText", mrCmd.getCommandText());
      result.add("context", context.toJson());

      // for old mr: #ODPS-65326
      // use GsonBuilder to serialize cause it will ignore null values by default
      String config = new GsonBuilder().create().toJson(result);

      out.write(config.getBytes(), 0, config.getBytes().length);
    } catch (IOException e) {
      throw new OdpsException("MapReduce write config error: " + e.getMessage());
    } catch (JsonParseException je) {
      throw new OdpsException("MapReduce write config error: " + je.getMessage());
    } finally {
      IOUtils.closeQuietly(out);
    }

    return true;
  }

  public static long getPID() {
    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
    return Long.parseLong(processName.split("@")[0]);
  }
}

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class GetTaskDetailsAction extends LogViewBaseAction {

  public static final String ACTION_NAME = "list";

  private static class FuxiInstance {
    public String logid;
    public String id;
    public String status;
    public long startTime;
    public int duration;
  }

  private static class FuxiTask {
    public String name;
    public List<FuxiInstance> instances;
  }

  private static class FuxiJob {
    public String name;
    public List<FuxiTask> tasks;
  }

  private String taskName;
  private boolean raw = false;
  private boolean sortByDuration = false;
  private int limit = -1;
  private boolean reverseOrder = false;
  private boolean selectRunning = false;
  private boolean selectFailed = false;

  @SuppressWarnings("static-access")
  public Options getOptions() {
    Options options = super.getOptions();
    options.addOption(OptionBuilder.withDescription("sort by duration").withLongOpt("duration")
        .create('d'));
    options.addOption(OptionBuilder.withDescription("maximum log process number")
        .withLongOpt("limit").hasArg().create('l'));
    options.addOption(OptionBuilder.withDescription("reverse the sort order of results")
        .withLongOpt("reverse").create('r'));
    options.addOption(OptionBuilder.withDescription("select running processes")
        .withLongOpt("running").create('R'));
    options.addOption(OptionBuilder.withDescription("select failed processes")
        .withLongOpt("failed").create('F'));
    return options;
  }

  public void configure(CommandLine cl) throws LogViewArgumentException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      throw new LogViewArgumentException("no instance id");
    } else if (args.length > 1) {
      throw new LogViewArgumentException("unexpected argument: " + args[1]);
    }
    String tokens[] = StringUtils.split(args[0], '/');
    ctx.setInstanceByName(tokens[0]);
    if (tokens.length == 2) {
      taskName = tokens[1];
    } else if (tokens.length > 2) {
      throw new LogViewArgumentException("invalid instance id: " + args[0]);
    }
    raw = cl.hasOption("raw");
    sortByDuration = cl.hasOption('d');
    if (cl.hasOption('l')) {
      limit = Integer.parseInt(cl.getOptionValue('l'));
    }
    reverseOrder = cl.hasOption('r');
    selectRunning = cl.hasOption('R');
    selectFailed = cl.hasOption('F');
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Instance inst = ctx.getInstance();
    if (taskName == null) {
      if (ctx.getTask() != null) {
        taskName = ctx.getTask();
      } else {
        taskName = deduceTaskName(inst);
      }
    }
    ctx.setTask(taskName);
    InputStream in = ctx.getTaskDetails(inst, taskName);
    if (raw) {
      byte buffer[] = new byte[5 * 1024];
      try {
        int sz = -1;
        while ((sz = in.read(buffer)) != -1) {
          String s = new String(buffer, 0, sz, "UTF-8");
          getWriter().writeResult(s);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      List<FuxiJob> jobs = loadJobsFromStream(in);
      ArrayList<FuxiInstance> flatInsts = new ArrayList<FuxiInstance>();
      for (FuxiJob job : jobs) {
        for (FuxiTask task : job.tasks) {
          for (FuxiInstance instance : task.instances) {
            // XXX: Cut off the redundant part of string:
            // "Odps/JobName/TaskName#Id"
            String[] tokens = StringUtils.split(instance.id, '/');
            try {
              String id = tokens[2];
              if (selectRunning || selectFailed) {
                if (selectRunning && instance.status.equals("Running")) {
                  flatInsts.add(instance);
                }
                if (selectFailed && instance.status.equals("Failed")) {
                  flatInsts.add(instance);
                }
              } else {
                flatInsts.add(instance);
              }
              ctx.getLogDir().put(id, instance.logid);
            } catch (IndexOutOfBoundsException e) {
              throw new ODPSConsoleException("Bad fuxi instance id " + instance.id);
            }
          }
        }
      }
      if (sortByDuration) {
        Collections.sort(flatInsts, new Comparator<FuxiInstance>() {
          public int compare(FuxiInstance a, FuxiInstance b) {
            FuxiInstance t1, t2;
            if (reverseOrder) {
              t2 = a;
              t1 = b;
            } else {
              t1 = a;
              t2 = b;
            }
            return t1.duration - t2.duration;
          }
        });
      }

      boolean first = true;
      for (int i = 0; i < flatInsts.size(); i++) {
        if (limit >= 0 && i >= limit) {
          break;
        }
        if (first) {
          // Print header
          getWriter().writeResult(
              String.format("%1$-20s%2$-25s%3$-10s%4$-15s", "ProcessID", "StartTime", "Duration",
                  "Status"));
          first = false;
        }
        FuxiInstance instance = flatInsts.get(i);
        String[] tokens = StringUtils.split(instance.id, '/');
        String startTimeStr = "-";
        if (instance.startTime > 0) {
          startTimeStr = ODPSConsoleUtils.formatDate(new Date(instance.startTime * 1000));
        }
        getWriter().writeResult(
            String.format("%1$-20s%2$-25s%3$-10s%4$-15s", tokens[2], startTimeStr,
                instance.duration + "s", instance.status));
      }
    }

  }

  @Override
  public String getActionName() {
    return ACTION_NAME;
  }

  @Override
  public String getHelpPrefix() {
    return "log list <instance id>";
  }

  // An huge hard-wired code parsing detail model from json

  private List<FuxiJob> loadJobsFromStream(InputStream in) throws ODPSConsoleException {
    JSONTokener tokener = new JSONTokener(new BufferedReader(new InputStreamReader(in)));
    try {
      JSONObject obj = new JSONObject(tokener);
      ArrayList<FuxiJob> jobs = new ArrayList<FuxiJob>();
      JSONObject mapReduceJson;
      try {
        mapReduceJson = obj.getJSONObject("mapReduce");
      } catch (JSONException e) {
        return jobs;
      }
      JSONArray jobsJson = mapReduceJson.getJSONArray("jobs");
      for (int i = 0; i < jobsJson.length(); i++) {
        jobs.add(getFuxiJobFromJson(jobsJson.getJSONObject(i)));
      }
      return jobs;
    } catch (JSONException e) {
      e.printStackTrace();
      throw new ODPSConsoleException("Bad json format");
    }
  }

  private FuxiJob getFuxiJobFromJson(JSONObject jobJson) throws JSONException {
    FuxiJob job = new FuxiJob();
    job.name = jobJson.getString("name");
    job.tasks = new ArrayList<FuxiTask>();
    JSONArray tasksJson = jobJson.getJSONArray("tasks");
    for (int i = 0; i < tasksJson.length(); i++) {
      job.tasks.add(getFuxiTaskFromJson(tasksJson.getJSONObject(i)));
    }
    return job;
  }

  private FuxiTask getFuxiTaskFromJson(JSONObject taskJson) throws JSONException {
    FuxiTask task = new FuxiTask();
    task.name = taskJson.getString("name");
    task.instances = new ArrayList<FuxiInstance>();
    JSONArray instancesJson = taskJson.getJSONArray("instances");
    for (int i = 0; i < instancesJson.length(); i++) {
      task.instances.add(getFuxiInstanceFromJson(instancesJson.getJSONObject(i)));
    }
    return task;
  }

  private FuxiInstance getFuxiInstanceFromJson(JSONObject instJson) throws JSONException {
    FuxiInstance inst = new FuxiInstance();
    inst.id = instJson.getString("id");
    inst.logid = instJson.getString("logId");
    inst.status = instJson.getString("status");
    inst.startTime = instJson.getLong("startTime");
    long endTime = instJson.getLong("endTime");
    inst.duration = (int) (endTime - inst.startTime);
    return inst;
  }
}

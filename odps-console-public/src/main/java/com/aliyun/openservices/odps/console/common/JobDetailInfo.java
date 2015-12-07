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

package com.aliyun.openservices.odps.console.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.google.gson.stream.JsonReader;

public class JobDetailInfo {

  private InstanceContext instanceContext = null;
  private Instance instance;
  private String taskName;

  private static class FuxiInstance {

    public String logid;
    public String id;
    public String status;
    public long startTime;
    public int duration;
    public String IpAndPath;
  }

  private static class FuxiTask {

    public String name;
    public List<FuxiInstance> instances;
    public List<FuxiTask> upTasks;
    public List<FuxiTask> downTasks;
  }

  private static class FuxiJob {

    public String name;
    public List<FuxiTask> tasks;

    public FuxiTask getFuxiTaskByName(String taskName) {
      for (FuxiTask task : tasks) {
        if (taskName.startsWith(task.name)) {
          return task;
        }
      }
      return null;
    }
  }

  public JobDetailInfo(InstanceContext instanceContext) throws OdpsException, ODPSConsoleException {
    this.instanceContext = instanceContext;
    this.instance = instanceContext.getInstance();
    if (taskName == null) {
      if (instanceContext.getTask() != null) {
        taskName = instanceContext.getTask();
      } else {
        taskName = deduceTaskName(this.instance);
      }
    }
    instanceContext.setTask(taskName);
  }

  public void printJobDetails() throws ODPSConsoleException {
    InputStream in = instanceContext.getTaskDetails(this.instance, taskName);
    List<FuxiJob> jobs = loadJobsFromStream(in);
    doPrintDetails(jobs);
  }

  public void doPrintDetails(List<FuxiJob> fuxiJobs) throws ODPSConsoleException {
    getWriter().writeResult("");

    for (FuxiJob job : fuxiJobs) {
      getWriter().writeResult(String.format("Job:%1$-20s", job.name));

      getWriter().writeResult("Stage(task):");
      for (FuxiTask task : job.tasks) {
        List<String> upTaskNames = new ArrayList<String>();
        List<String> downTaskNames = new ArrayList<String>();
        if (task.upTasks != null && task.upTasks.size() > 0) {
          for (FuxiTask eachtask : task.upTasks) {
            upTaskNames.add(eachtask.name);
          }
        }
        if (task.downTasks != null && task.downTasks.size() > 0) {
          for (FuxiTask eachtask : task.downTasks) {
            downTaskNames.add(eachtask.name);
          }
        }
        getWriter().writeResult(String.format("    %1$-20s %2$-20s -> this -> %3$-20s", task.name,
                                              upTaskNames, downTaskNames));
        printInstances(task.instances);
      }

      List<FuxiInstance> criticalPath = this.getCriticalPath(job);
      getWriter().writeResult("Critical Instance Path:");
      printInstances(criticalPath);
    }

    getWriter().writeResult("");
  }

  protected static String deduceTaskName(Instance inst) throws ODPSConsoleException, OdpsException {
    Map<String, Instance.TaskStatus> ss = inst.getTaskStatus();
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

  private DefaultOutputWriter getWriter() {
    return this.instanceContext.getSession().getOutputWriter();
  }

  private List<FuxiInstance> getCriticalPath(FuxiJob fuxiJob) {
    getWriter().writeResult("");

    List<FuxiInstance> path = new ArrayList<FuxiInstance>();
    List<FuxiTask> currTasks = new ArrayList<FuxiTask>();
    // Assume only one final output task in whole fuxi job
    currTasks.add(fuxiJob.tasks.get(fuxiJob.tasks.size() - 1));

    while (currTasks != null && currTasks.size() > 0) {
      FuxiTask slowestTask = null;
      FuxiInstance slowestInstance = null;
      for (FuxiTask task : currTasks) {
        for (FuxiInstance instance : task.instances) {
          if (slowestInstance == null ||
              (instance.startTime + instance.duration) > (slowestInstance.startTime
                                                          + slowestInstance.duration)) {
            slowestInstance = instance;
            slowestTask = task;
          }
        }
      }

      path.add(0, slowestInstance);
      currTasks = slowestTask.upTasks;
    }

    return path;
  }

  private void printInstances(List<FuxiInstance> instances) {
    for (FuxiInstance instance : instances) {
      String[] tokens = StringUtils.split(instance.id, '/');
      String startTimeStr = "-";
      String endTimeStr = "-";
      if (instance.startTime > 0) {
        startTimeStr = ODPSConsoleUtils.formatDate(new Date(instance.startTime * 1000));
        endTimeStr =
            ODPSConsoleUtils.formatDate(new Date((instance.startTime + instance.duration) * 1000));
      }
      getWriter().writeResult(
          String.format("      %1$-20s%2$-25s%3$-25s%4$-15s%5$-15s%6$-18s", tokens[2], startTimeStr,
                        endTimeStr,
                        instance.duration + "s", instance.status,
                        instance.IpAndPath.split(",")[0]));
    }
  }

  private List<FuxiJob> loadJobsFromStream(InputStream in) throws ODPSConsoleException {
    boolean debug = true;
    ArrayList<FuxiJob> jobs = new ArrayList<FuxiJob>();
    if (debug) {
      try {
        JsonReader reader = new JsonReader(new InputStreamReader(in, "UTF-8"));
        reader.beginObject();
        while (reader.hasNext()) {
          String name = reader.nextName();
          if (name.equals("mapReduce")) {
            reader.beginObject();
            while (reader.hasNext()) {
              String nameInMapReduce = reader.nextName();
              if (nameInMapReduce.equals("jobs")) {
                reader.beginArray();
                while (reader.hasNext()) {
                  jobs.add(getFuxiJobFromJson(reader));
                }
                reader.endArray();
              } else if (nameInMapReduce.equals("jsonSummary")) {
                getInfoFromJsonSummary(jobs, reader.nextString());
              } else {
                reader.skipValue();
              }
            }
            reader.endObject();
          } else {
            reader.skipValue();
          }
        }
        reader.endObject();
      } catch (IOException e) {
        e.printStackTrace();
        throw new ODPSConsoleException("Bad json format");
      }
    }

    return jobs;
  }

  private FuxiJob getFuxiJobFromJson(JsonReader reader) throws IOException {
    FuxiJob job = new FuxiJob();

    reader.beginObject();
    while (reader.hasNext()) {
      String nameInJob = reader.nextName();
      if (nameInJob.equals("name")) {
        job.name = reader.nextString();
      } else if (nameInJob.equals("tasks")) {
        reader.beginArray();
        job.tasks = new ArrayList<FuxiTask>();
        while (reader.hasNext()) {
          job.tasks.add(getFuxiTaskFromJson(reader));
        }
        reader.endArray();
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return job;
  }

  private FuxiTask getFuxiTaskFromJson(JsonReader reader) throws IOException {
    FuxiTask task = new FuxiTask();

    reader.beginObject();
    while (reader.hasNext()) {
      String nameInTask = reader.nextName();
      if (nameInTask.equals("name")) {
        task.name = reader.nextString();
      } else if (nameInTask.equals("instances")) {
        task.instances = new ArrayList<FuxiInstance>();
        task.upTasks = new ArrayList<FuxiTask>();
        task.downTasks = new ArrayList<FuxiTask>();
        reader.beginArray();
        while (reader.hasNext()) {
          task.instances.add(getFuxiInstanceFromJson(reader));
        }
        reader.endArray();
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return task;
  }

  private FuxiInstance getFuxiInstanceFromJson(JsonReader reader) throws IOException {
    FuxiInstance inst = new FuxiInstance();

    reader.beginObject();
    long endTime = 0;
    while (reader.hasNext()) {
      String nameInInstance = reader.nextName();
      if (nameInInstance.equals("id")) {
        inst.id = reader.nextString();
      } else if (nameInInstance.equals("logId")) {
        inst.logid = reader.nextString();
        inst.IpAndPath = this.decodeLogId(inst.logid);
      } else if (nameInInstance.equals("startTime")) {
        inst.startTime = reader.nextLong();
      } else if (nameInInstance.equals("endTime")) {
        endTime = reader.nextLong();
      } else if (nameInInstance.equals("status")) {
        inst.status = reader.nextString();
      } else {
        reader.skipValue();
      }
    }
    inst.duration = (int) (endTime - inst.startTime);
    reader.endObject();
    return inst;
  }

  private String decodeLogId(String input) {
    if (input == null || input.isEmpty()) {
      return "";
    }
    String tmp = new String(org.apache.commons.codec.binary.Base64.decodeBase64(input));
    if (tmp.length() <= 4) {
      return "";
    }
    tmp = tmp.substring(1, tmp.length() - 3) + tmp.substring(0, 1);
    return new String(org.apache.commons.codec.binary.Base64.decodeBase64(tmp));
  }

  private void getInfoFromJsonSummary(List<FuxiJob> fuxiJobs, String jsonSummaryContent)
      throws IOException {
    jsonSummaryContent = jsonSummaryContent.replaceAll("\n", " ");
    jsonSummaryContent = jsonSummaryContent.replaceAll("\t", " ");
    jsonSummaryContent = jsonSummaryContent.replace("\\\"", "\"");

    JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(jsonSummaryContent.getBytes())));

    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("jobs")) {
        reader.beginArray();

        int jobCount = 0;
        // Get more info for each job
        while (reader.hasNext()) {
          reader.beginObject();
          FuxiJob job = fuxiJobs.get(jobCount);
          while (reader.hasNext()) {
            String nameInJobs = reader.nextName();
            if (nameInJobs.equals("tasks")) {
              reader.beginObject();

              int taskCount = 0;
              // Get more info for each task
              while (reader.hasNext()) {
                String taskName = reader.nextName();
                FuxiTask task = job.tasks.get(taskCount);

                // Get the downstream tasks info
                reader.beginObject();
                while (reader.hasNext()) {
                  if (reader.nextName().equals("output_record_counts")) {
                    List<String> downTasks = new ArrayList<String>();
                    reader.beginObject();
                    while (reader.hasNext()) {
                      downTasks.add(reader.nextName());
                      reader.skipValue();
                    }
                    reader.endObject();
                    addUpAndDownTasks(job, task.name, downTasks);
                  } else {
                    reader.skipValue();
                  }
                }
                reader.endObject();
                taskCount++;
              }
              reader.endObject();
            } else {
              reader.skipValue();
            }
          }
          reader.endObject();
          jobCount++;
        }
        reader.endArray();
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
  }

  private void addUpAndDownTasks(FuxiJob job, String currTask, List<String> downTasks) {
    for (FuxiTask task : job.tasks) {
      if (task.name.equalsIgnoreCase(currTask)) {
        for (String taskName : downTasks) {
          task.downTasks.add(job.getFuxiTaskByName(taskName));
        }
      } else {
        for (String downTask : downTasks) {
          if (downTask.startsWith(task.name)) {
            task.upTasks.add(job.getFuxiTaskByName(currTask));
          }
        }
      }
    }
  }
}


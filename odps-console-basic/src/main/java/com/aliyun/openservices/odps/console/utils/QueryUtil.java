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

package com.aliyun.openservices.odps.console.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.commands.SetCommand;

import jline.console.UserInterruptException;

/**
 * 执行query的工具类
 * 
 * @author shuman.gansm
 * **/
public class QueryUtil {

  /**
   * 批量执行sql
   * */
  public static void batchExecuteSql(Odps odps, List<String> sqlList, int batchNumber,
      ExecutionContext sessionContext) throws OdpsException {

    if (batchNumber < 1) {
      batchNumber = 999;
    }

    sessionContext.getOutputWriter().writeError("start batch sql..., batch:" + batchNumber);

    // 根据batchNumber，分成多个job instatnce
    List<String> batchList = new ArrayList<String>();
    int i = 1;
    for (String sql : sqlList) {

      batchList.add(sql);

      if (i == batchNumber) {

        runNewInstatnce(odps, batchList, sessionContext);
        // 创建新的batch
        i = 1;
        batchList = new ArrayList<String>();
      }
      i++;
    }

    if (batchList.size() > 0) {
      // 最后一次
      runNewInstatnce(odps, batchList, sessionContext);
    }

  }

  /**
   * 起一个新的job instatnce
   * */
  private static void runNewInstatnce(Odps odps, List<String> sqlList,
      ExecutionContext sessionContext) throws OdpsException {

    List<Task> tasks = new ArrayList<Task>();

    int i = 0;
    for (String sql : sqlList) {
      SQLTask sqltask = new SQLTask();
      sqltask.setName("bTask_" + i);
      sqltask.setQuery(sql + ";");
      tasks.add(sqltask);
      i++;
    }

    Job job = new Job();
    job.setName("bJob_" + Calendar.getInstance().getTimeInMillis());

    job.setComment("batch");
    // job.setPriority(10);
    for (Task task : tasks) {
      job.addTask(task);
    }
    /*
     * job.setDag(new SequenceDag()); job.save(SaveOption.CREATE_NEW);
     */
    Instance instance = odps.instances().create(job);
    batchStatus(job, instance, sessionContext);
  }

  private static void batchStatus(Job job, Instance instance, ExecutionContext sessionContext)
      throws OdpsException {

    sessionContext.getOutputWriter().writeError("ID = " + instance.getId());
    Instance.Status instanceStatus = Instance.Status.RUNNING;
    while (!instanceStatus.equals(Instance.Status.TERMINATED)) {

      try {
        Thread.sleep(1000 * 10);
        instanceStatus = instance.getStatus();
        Date date = new Date();
        SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sessionContext.getOutputWriter().writeError(sim.format(date) + "\t" + instanceStatus);
      } catch (Exception e1) {
        e1.printStackTrace();
      }
    }

    Map<String, TaskStatus> statusMap = null;
    try {
      statusMap = instance.getTaskStatus();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // 最后得到detail信息来获得task的运行状态
    while (statusMap == null) {
      try {
        Thread.sleep(1000 * 5);
      } catch (InterruptedException e) {
        throw new UserInterruptException(e.getMessage());
      }
      statusMap = instance.getTaskStatus();
    }

    boolean status = true;
    StringBuilder failedBuilder = new StringBuilder();

    for (Task task : job.getTasks()) {
      TaskStatus taskStatus = statusMap.get(task.getName());
      if (taskStatus == null) {
        continue;
      }

      if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
        status = false;

        failedBuilder.append("message:" + instance.getTaskResults().get(task.getName()) + "\r\nsql:"
            + ((SQLTask) task).getQuery() + "\r\n");
      } else {
        // System.out.println(instance.getResult().get(task.getName()));
      }
    }

    if (!status) {
      throw new OdpsException("[ task status: Failed]" + failedBuilder.toString());
    }

  }

  public static HashMap<String, String> getTaskConfig() {

    // get session config
    HashMap<String, String> taskConfig = new HashMap<String, String>();

    if (!SetCommand.setMap.isEmpty()) {
      JSONObject jsObj = new JSONObject(SetCommand.setMap);
      if(!jsObj.toString().equals("{\"empty\":false}")){
        taskConfig.put("settings", jsObj.toString());
      }
    }

    if (!SetCommand.aliasMap.isEmpty()) {
      JSONObject jsObj = new JSONObject(SetCommand.aliasMap);
      taskConfig.put("aliases", jsObj.toString());
    }

    return taskConfig;
  }
  /**
   * 判断sql是否为insert并且有动态partition
   * */
  public static boolean isOperatorDisabled(String sql){
      
      String upSql = sql.toUpperCase();
      
      if (upSql.matches("^INSERT\\s+INTO.*")){
          return true;
      }
      
      //dy patition
      if (upSql.indexOf("INSERT ") >=0 && upSql.indexOf(" PARTITION") >=0 ){
          
          //split partition
          String[] partitions =  upSql.split(" PARTITION");
          
          for (int i = 0; i < partitions.length; i++){
              String temp = partitions[i].trim();
              
              if (temp.startsWith("(") && temp.indexOf(")") > 0){
                  
                  //get partition spec
                  String partitionStr = temp.substring(0, temp.indexOf(")"));
                  String[] partitionSpcs = partitionStr.split(",");
                  String lastPartitionSpc = partitionSpcs[partitionSpcs.length -1 ];
                  if (lastPartitionSpc.indexOf("=") == -1){
                      //只需要判断未及分区为动态的就可以了
                      return true;
                  }
              }
          }
      }
      
      return false;
  }

}

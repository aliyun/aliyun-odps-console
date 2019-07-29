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

import java.util.HashMap;

import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.google.gson.GsonBuilder;

/**
 * 执行query的工具类
 * 
 * @author shuman.gansm
 * **/
public class QueryUtil {

  public static HashMap<String, String> getTaskConfig() {

    // get session config
    HashMap<String, String> taskConfig = new HashMap<String, String>();

    if (!SetCommand.setMap.isEmpty()) {
      taskConfig.put("settings",
              new GsonBuilder().disableHtmlEscaping().create().toJson(SetCommand.setMap));
    }

    if (!SetCommand.aliasMap.isEmpty()) {
      taskConfig.put("aliases",
              new GsonBuilder().disableHtmlEscaping().create().toJson(SetCommand.aliasMap));
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

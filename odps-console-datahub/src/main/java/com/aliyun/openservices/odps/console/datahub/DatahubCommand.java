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

package com.aliyun.openservices.odps.console.datahub;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.ReplicatorStatus;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;
import jline.console.UserInterruptException;

/**
 * @author dongxiao.dx
 * */
public class DatahubCommand extends AbstractCommand {

  long shardNumber;
  String projectName;
  String tableName;
  String endpoint;
  DatahubMethod method;

  public enum DatahubMethod {
    Load,
    Unload,
    ShardStatus,
    ShardList,
    ReplicateStatus
  }

  public static final String[] HELP_TAGS = new String[]{"datahub", "shard", "replicate", "hub", "shards"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: hub load [num] shards on [<prj_name.>tbl_name] [endpoint];");
    out.println("       hub unload shard on [<prj_name.>tbl_name] [endpoint];");
    out.println("       hub shard status on [<prj_name.>tbl_name] [endpoint];");
    out.println("       hub shard list on [<prj_name.>tbl_name] [endpoint];");
    out.println("       hub replicate status of shard [id] on [<prj_name.>tbl_name] [endpoint];");
  }

  public DatahubCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public DatahubCommand(long shardNumber, String projectName, String tableName, String endpoint, DatahubMethod method,
                        String commandText, ExecutionContext context) {
    super(commandText, context);
    this.shardNumber = shardNumber;
    this.projectName = projectName;
    this.tableName = tableName;
    this.endpoint = endpoint;
    this.method = method;
  }

  private long getLoadedShardNumber(StreamClient client) throws TunnelException{
    try {
      HashMap<Long, StreamClient.ShardState> shardStatusMap = client.getShardStatus();
      Iterator iter = shardStatusMap.entrySet().iterator();
      long loaded_shard_num = 0;
      while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
        StreamClient.ShardState status = (StreamClient.ShardState) entry.getValue();

        if (status == StreamClient.ShardState.LOADED) {
          loaded_shard_num++;
        }
      }
      return loaded_shard_num;
    } catch (Exception e) {
      throw new TunnelException(e.getMessage(), e);
    }
  }

  private static String[] parseProjectTable(String cmdString) throws ODPSConsoleException{
    String[] prj_tbl = cmdString.split("\\.");
    if (prj_tbl.length != 2 || prj_tbl[0].equals("") || prj_tbl[1].equals("")) {
      throw new ODPSConsoleException("Invalid parameters - Wrong project or table format.");
    }
    return prj_tbl;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    ExecutionContext context = getContext();
    Odps odps = getCurrentOdps();
    TableTunnel tunnel = new TableTunnel(odps);

    if (this.endpoint != "") {
      tunnel.setEndpoint(this.endpoint);
    } else if (context.getDatahubEndpoint() != null) {
      tunnel.setEndpoint(context.getDatahubEndpoint());
    } else {
      throw new ODPSConsoleException("hub_endpoint not set!");
    }

    try {
      StreamClient client = tunnel.createStreamClient(projectName, tableName);

      if (method == DatahubMethod.Load){
        if (shardNumber <= 0) {
          throw new ODPSConsoleException("ShardNumber must > 0!");
        }
        client.loadShard(shardNumber);
        while (true){
          Long loaded_shard_num = getLoadedShardNumber(client);
          System.err.println("Shard load status: " + loaded_shard_num + "/" + this.shardNumber);
          if (loaded_shard_num == this.shardNumber) {
            break;
          }
          Thread.sleep(2000L);
        }
      } else if (method == DatahubMethod.Unload) {
        Long loaded_shard_num = getLoadedShardNumber(client);
        client.loadShard(0L);
        while (true){
          Long curLoaded_shard_num = getLoadedShardNumber(client);
          System.err.println("Shard unload status: " + (loaded_shard_num - curLoaded_shard_num) + "/" + loaded_shard_num);
          if (curLoaded_shard_num == 0) {
            break;
          }
          Thread.sleep(2000L);
        }
      } else if (method == DatahubMethod.ShardStatus) {
        HashMap<Long, StreamClient.ShardState> status = client.getShardStatus();
        Iterator iter = status.entrySet().iterator();
        while (iter.hasNext()){
          Map.Entry entry = (Map.Entry) iter.next();
          System.err.println("ShardId: " + entry.getKey() + " Status: " + entry.getValue());
        }
      } else if (method == DatahubMethod.ShardList) {
        List<Long> shards = client.getShardList();
        String shardStr = "ShardId: [";
        for (Long shardId : shards){
          shardStr += shardId + ",";
        }
        shardStr = shardStr.substring(0, shardStr.lastIndexOf(','));
        System.err.println(shardStr + "]");
      } else if (method == DatahubMethod.ReplicateStatus) {
        if (shardNumber < 0) {
          throw new ODPSConsoleException("ShardNumber must > 0!");
        }
        ReplicatorStatus status = client.QueryReplicatorStatus(this.shardNumber);
        System.err.println("LastReplicatedPackId :" + status.GetLastReplicatedPackId());
        System.err.println("LastReplicatedPackTimeStamp :" + status.GetLastReplicatedPackTimeStamp());
      } else {
        throw new ODPSConsoleException("Unsupported method!");
      }
    } catch (InterruptedException e) {
      throw new UserInterruptException(e.getMessage());
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage());
    }
    System.err.println("OK");
  }

  public static DatahubCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    AntlrObject antlr = new AntlrObject(commandString);
    String[] args = antlr.getTokenStringArray();

    if (args == null || args.length == 0) {
      return null;
    }

    if (args[0].toUpperCase().equals("HUB")){
      long shardNumber = 0;
      String projectName = "";
      String tableName = "";
      String endpoint = "";
      DatahubMethod method = DatahubMethod.Load;

      if (args.length >= 6
          && args[1].toUpperCase().equals("LOAD")
          && args[3].toUpperCase().equals("SHARDS")
          && args[4].toUpperCase().equals("ON")) {

        shardNumber = Long.parseLong(args[2]);
        method = DatahubMethod.Load;
        String[] prj_tbl = DatahubCommand.parseProjectTable(args[5]);
        projectName = prj_tbl[0];
        tableName = prj_tbl[1];
        if (args.length == 7){
          endpoint = args[6];
        }

      } else if (args.length >=5
          && args[1].toUpperCase().equals("UNLOAD")
          && args[2].toUpperCase().equals("SHARD")
          && args[3].toUpperCase().equals("ON")) {

        method = DatahubMethod.Unload;
        String[] prj_tbl = DatahubCommand.parseProjectTable(args[4]);
        projectName = prj_tbl[0];
        tableName = prj_tbl[1];
        if (args.length == 6) {
          endpoint = args[5];
        }

      } else if (args.length >=5
          && args[1].toUpperCase().equals("SHARD")
          && args[2].toUpperCase().equals("STATUS")
          && args[3].toUpperCase().equals("ON")) {

        method = DatahubMethod.ShardStatus;
        String[] prj_tbl = DatahubCommand.parseProjectTable(args[4]);
        projectName = prj_tbl[0];
        tableName = prj_tbl[1];
        if (args.length == 6) {
          endpoint = args[5];
        }

      } else if (args.length >=5
          && args[1].toUpperCase().equals("SHARD")
          && args[2].toUpperCase().equals("LIST")
          && args[3].toUpperCase().equals("ON")) {

        method = DatahubMethod.ShardList;
        String[] prj_tbl = DatahubCommand.parseProjectTable(args[4]);
        projectName = prj_tbl[0];
        tableName = prj_tbl[1];
        if (args.length == 6) {
          endpoint = args[5];
        }

      } else if (args.length >=8
          && args[1].toUpperCase().equals("REPLICATE")
          && args[2].toUpperCase().equals("STATUS")
          && args[3].toUpperCase().equals("OF")
          && args[4].toUpperCase().equals("SHARD")
          && args[6].toUpperCase().equals("ON")) {

        shardNumber = Long.parseLong(args[5]);
        method = DatahubMethod.ReplicateStatus;
        String[] prj_tbl = DatahubCommand.parseProjectTable(args[7]);
        projectName = prj_tbl[0];
        tableName = prj_tbl[1];
        if (args.length == 9) {
          endpoint = args[8];
        }

      }
      else {
        throw new ODPSConsoleException("Unsupported method! Please input help hub to get help.");
      }

      DatahubCommand command = new DatahubCommand(shardNumber, projectName, tableName, endpoint, method, commandString,
          sessionContext);

      return command;
    }

    return null;

  }

  public static void main(String args[]) throws Exception {

    DatahubCommand command = DatahubCommand
            .parse(
                    "hub load 3 shards on test_dx.test_log",
                    ExecutionContext.load(null));
    command.run();
    Thread.sleep(1000L);
    command = DatahubCommand
            .parse(
                    "hub shard status on test_dx.test_log",
                    ExecutionContext.load(null));
    command.run();
    Thread.sleep(1000L);
    command = DatahubCommand
            .parse(
                    "hub shard list on test_dx.test_log",
                    ExecutionContext.load(null));
    command.run();
    Thread.sleep(1000L);
    command = DatahubCommand
            .parse(
                    "hub replicate status of shard 0 on test_dx.test_log",
                    ExecutionContext.load(null));
    command.run();
    Thread.sleep(1000L);
    command = DatahubCommand
            .parse(
                    "hub unload shard on test_dx.test_log",
                    ExecutionContext.load(null));
    command.run();
  }
}

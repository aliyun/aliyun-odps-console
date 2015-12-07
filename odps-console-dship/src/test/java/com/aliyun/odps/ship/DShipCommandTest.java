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

package com.aliyun.odps.ship;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/5/19.
 */
public class DShipCommandTest {

  @Test
  public void testDshipMultiThreads() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete("instances");
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    odps.tables().create("instances", tableSchema, true);
    odps.tables().get("instances").truncate();
    String file = this.getClass().getResource("/many_record.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " instances -cp=false -threads=4  -bs=1", context);
    command.run();

    String outputFile = new File(file).getParent() + "/abcd.txt";

    command = DShipCommand.parse("tunnel d instances " + outputFile + " -cp=false -threads=4 -limit=580000", context);
    command.run();
  }

  @Test
  public void testDshipMultiPartitions() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    odps.tables().delete("multi_partitions");

    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("pt", OdpsType.BIGINT));

    odps.tables().create("multi_partitions", tableSchema, true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=1"), true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=2"), true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=3"), true);

    String file = this.getClass().getResource("/many_record.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=1 -cp=false -threads=4  -bs=1", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=2 -cp=false -threads=4  -bs=1", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=3 -cp=false -threads=4  -bs=1", context);
    command.run();

    String outputFile = new File(file).getParent() + "/abcd.txt";

    command = DShipCommand.parse("tunnel d multi_partitions " + outputFile + " -cp=false -threads=2 -limit=1700000", context);
    command.run();
  }


  @Test
  public void testHelpCommand() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    DShipCommand command = DShipCommand.parse("tunnel help upload", context);
    String result = ODPSConsoleUtils.runCommand(command);
    System.out.println(result);
    assertFalse(result.contains("--"));
  }
}

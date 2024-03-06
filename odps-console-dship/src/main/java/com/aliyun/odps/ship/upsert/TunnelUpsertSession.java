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

package com.aliyun.odps.ship.upsert;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.jline.reader.UserInterruptException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.CommandType;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.RecordConverter;
import com.aliyun.odps.ship.upload.TunnelUpdateSession;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class TunnelUpsertSession implements TunnelUpdateSession {

  private TableTunnel.UpsertSession upsert;
  private Record record;

  public TunnelUpsertSession() throws ODPSConsoleException, OdpsException, IOException {
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String schemaName = DshipContext.INSTANCE.get(Constants.SCHEMA);
    String tableName = DshipContext.INSTANCE.get(Constants.TABLE);
    String partitionSpec = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    String resumeUpsertId = DshipContext.INSTANCE.get(Constants.RESUME_UPSERT_ID);

    Odps odps = OdpsConnectionFactory.createOdps(DshipContext.INSTANCE.getExecutionContext());

    PartitionSpec ps = partitionSpec == null ? null : new PartitionSpec(partitionSpec);
    TableTunnel tunnel = new TableTunnel(odps);

    if (DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT) != null) {
      tunnel.setEndpoint(DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT));
    } else if (StringUtils.isNotEmpty(
        DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint())) {
      tunnel.setEndpoint(DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint());
    }

    if (StringUtils.isEmpty(tableProject)) {
      tableProject = odps.getDefaultProject();
    }
    if (ps != null && "true".equalsIgnoreCase(
        DshipContext.INSTANCE.get(Constants.AUTO_CREATE_PARTITION))) {
      Table t = odps.tables().get(tableProject, schemaName, tableName);
      if (!t.hasPartition(ps)) {
        System.err.println("Create partition " + ps);
        t.createPartition(ps);
      }
    }

    TableTunnel.UpsertSession.Builder builder =
        tunnel.buildUpsertSession(tableProject, tableName)
            .setSchemaName(schemaName).setPartitionSpec(ps);

    if (StringUtils.isNotEmpty(resumeUpsertId)) {
      builder.setUpsertId(resumeUpsertId);
    }
    upsert = builder.build();
    // upsert requires ordered input, so the thread is set to 1.
    DshipContext.INSTANCE.put(Constants.THREADS, "1");
    // upsert do not support scan
    DshipContext.INSTANCE.put(Constants.SCAN, "false");
    System.err.println("Upload session: " + upsert.getId());
  }

  public TableSchema getSchema() {
    return upsert.getSchema();
  }

  public String getSessionId() {
    return upsert.getId();
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.upsert;
  }

  public RecordWriter getWriter(long bId) throws IOException, TunnelException {
    return new UpsertRecordWriter(upsert);
  }

  public void complete(List<Long> bList) throws TunnelException, IOException {
    int retry = 1;
    try {
      while (true) {
        try {
          upsert.commit(false);
          break;
        } catch (TunnelException e) {
          System.err.println("Exception occurred while commit upsert session: " + e.getMessage()
                             + ", retry(" + retry + "/" + Constants.RETRY_LIMIT + ")");
          retry++;
          if (retry > Constants.RETRY_LIMIT) {
            throw e;
          }
        }
        try {
          Thread.sleep(Constants.RETRY_INTERNAL);
        } catch (InterruptedException e) {
          throw new UserInterruptException(e.getMessage());
        }
      }
    } finally {
      upsert.close();
    }
  }

  public void initRecord() {
    record = upsert.newRecord();
  }

  public Record getRecord(RecordConverter recordConverter, byte[][] textRecord)
      throws UnsupportedEncodingException, ParseException {
    return recordConverter.parse(record, textRecord);
  }


  public TableTunnel.UpsertSession getUpsert() {
    return upsert;
  }
}

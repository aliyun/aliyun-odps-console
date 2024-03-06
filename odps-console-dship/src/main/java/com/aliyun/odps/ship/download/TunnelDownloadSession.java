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

package com.aliyun.odps.ship.download;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class TunnelDownloadSession {

  private DownloadSession tableDownload = null;
  private InstanceTunnel.DownloadSession instanceDownload = null;
  boolean isInstanceTunnel = false;
  private List<Column> selectedColumns = null;
  private TableSchema schema;
  private String downloadId;
  private SessionHistory sessionHistory;
  private long totalLines;

  protected TunnelDownloadSession(long lines) {
    totalLines = lines;
  }

  public TunnelDownloadSession(String instanceId)
      throws TunnelException, ODPSConsoleException, FileNotFoundException {
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    Odps odps = OdpsConnectionFactory.createOdps(DshipContext.INSTANCE.getExecutionContext());
    InstanceTunnel tunnel = new InstanceTunnel(odps);

    if (DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT) != null) {
      tunnel.setEndpoint(DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT));
    } else if (StringUtils.isNotEmpty(
        DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint())) {
      tunnel.setEndpoint(DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint());
    }

    if (tableProject == null) {
      tableProject = odps.getDefaultProject();
    }

    instanceDownload = tunnel.createDownloadSession(tableProject, instanceId);
    sessionHistory = SessionHistoryManager.createSessionHistory(instanceDownload.getId());
    totalLines = instanceDownload.getRecordCount();
    schema = instanceDownload.getSchema();
    downloadId = instanceDownload.getId();

    isInstanceTunnel = true;
    initSelectColumns();
  }

  public TunnelDownloadSession(String tableName, PartitionSpec ps)
      throws OdpsException, ODPSConsoleException, IOException {
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String schemaName = DshipContext.INSTANCE.get(Constants.SCHEMA);
    Odps odps = OdpsConnectionFactory.createOdps(DshipContext.INSTANCE.getExecutionContext());
    TableTunnel tunnel = new TableTunnel(odps);

    if (DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT) != null) {
      tunnel.setEndpoint(DshipContext.INSTANCE.get(Constants.TUNNEL_ENDPOINT));
    } else if (StringUtils.isNotEmpty(
        DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint())) {
      tunnel.setEndpoint(DshipContext.INSTANCE.getExecutionContext().getTunnelEndpoint());
    }

    if (tableProject == null) {
      tableProject = odps.getDefaultProject();
    }
    TableTunnel.DownloadSessionBuilder
        builder =
        tunnel.buildDownloadSession(tableProject, tableName).setSchemaName(schemaName)
            .setAsyncMode(true).setWaitAsyncBuild(false);
    if (ps == null) {
      tableDownload = builder.build();
    } else {
      tableDownload = builder.setPartitionSpec(ps).build();
    }
    sessionHistory = SessionHistoryManager.createSessionHistory(tableDownload.getId());
    String rapInstanceId = tableDownload.getRAPInstanceId();
    if (rapInstanceId != null) {
      while (rapInstanceId.isEmpty()) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        // get status will trigger reload
        TableTunnel.DownloadStatus status = tableDownload.getStatus();
        rapInstanceId = tableDownload.getRAPInstanceId();
        if (status != TableTunnel.DownloadStatus.INITIATING) {
          break;
        }
      }
      if (rapInstanceId.isEmpty()) {
        throw new OdpsException(
            "Create TunnelDownloadSession error, failed to create RAP instance. session id: "
            + tableDownload.getId());
      }
      Instance sqlInstance = odps.instances().get(rapInstanceId);
      log("\nDue to the presence of row-level permission rules on current table, tunnel download requires run SQL task. Task info:");
      log("ID = " + rapInstanceId);
      log("Log view:\n" + odps.logview().generateLogView(sqlInstance, 7 * 24) + "\n");
      sqlInstance.waitForSuccess();
    }
    builder.wait(tableDownload, 2, 2 * 60);
    totalLines = tableDownload.getRecordCount();
    schema = tableDownload.getSchema();
    downloadId = tableDownload.getId();
    initSelectColumns();
  }

  private void initSelectColumns() {
    String columnNames = DshipContext.INSTANCE.get(Constants.COLUMNS_NAME);
    String columnIndexes = DshipContext.INSTANCE.get(Constants.COLUMNS_INDEX);

    if (columnIndexes != null || columnNames != null) {
      boolean isIndex = (columnIndexes != null);
      String columns = isIndex ? columnIndexes : columnNames;
      selectedColumns = new ArrayList<Column>();

      try {
        for (String name : columns.split(",")) {
          if (!isIndex) {
            // trim and ignore case
            selectedColumns.add(schema.getColumn(name.trim().toLowerCase()));
          } else {
            selectedColumns.add(schema.getColumn(Integer.parseInt(name.trim())));
          }
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Parse selected columns: %s failed, %s", columns, e.getMessage()), e);
      }
      System.err.println(String.format("Download %d columns, %s: %s", selectedColumns.size(),
                                       isIndex ? "column indexes are" : "column names are",
                                       columns));

      // update schema
      schema.setColumns(selectedColumns);
    }
  }


  public DshipRecordReader getRecordReader(Long start, Long end)
      throws IOException, TunnelException {
    return new DshipRecordReader(this, start, end, selectedColumns);
  }

  public TableSchema getSchema() {
    return schema;
  }

  public String getDownloadId() {
    return downloadId;
  }

  public long getTotalLines() {
    return totalLines;
  }

  public SessionHistory getSessionHistory() {
    return sessionHistory;
  }

  public RecordReader openRecordReader(long start, long count, boolean compress,
                                       List<Column> columns) throws TunnelException, IOException {
    return isInstanceTunnel ? instanceDownload.openRecordReader(start, count, compress, columns)
                            : tableDownload.openRecordReader(start, count, compress, columns);
  }

  private void log(String str) throws IOException {
    sessionHistory.log(str);
    System.err.println(str);
  }
}

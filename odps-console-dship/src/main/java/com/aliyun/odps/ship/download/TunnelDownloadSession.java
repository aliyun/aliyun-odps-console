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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
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
  private long totalLines;

  //Construct for ut
  protected TunnelDownloadSession(long lines) {
    totalLines = lines;
  }

  // construct for tunnel download instance
  public TunnelDownloadSession(String instanceId) throws TunnelException, IOException, ODPSConsoleException {
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
    totalLines = instanceDownload.getRecordCount();
    schema = instanceDownload.getSchema();
    downloadId = instanceDownload.getId();

    isInstanceTunnel = true;
    initSelectColumns();
  }

  // construct for tunnel download table
  public TunnelDownloadSession(String tableName, PartitionSpec ps)
      throws TunnelException, IOException, ODPSConsoleException {
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
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

    if (ps == null) {
      tableDownload = tunnel.createDownloadSession(tableProject, tableName);
    } else {
      tableDownload = tunnel.createDownloadSession(tableProject, tableName, ps);
    }

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
                                       isIndex ? "column indexes are" : "column names are", columns));

      // update schema
      schema.setColumns(selectedColumns);
    }
  }


  public DshipRecordReader getRecordReader(Long start, Long end) throws IOException, TunnelException {
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

  public RecordReader openRecordReader(long start, long count, boolean compress,
                                       List<Column> columns) throws TunnelException, IOException {
    return isInstanceTunnel ? instanceDownload.openRecordReader(start, count, compress, columns) :
           tableDownload.openRecordReader(start, count, compress, columns);
  }
}

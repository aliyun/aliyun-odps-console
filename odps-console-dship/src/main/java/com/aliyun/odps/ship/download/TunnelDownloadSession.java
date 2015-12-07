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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class TunnelDownloadSession {

  DownloadSession download;
  private long totalLines;

  //Construct for ut
  protected TunnelDownloadSession(long lines) {
    totalLines = lines;
  }

  public TunnelDownloadSession(PartitionSpec ps) throws TunnelException, IOException, ODPSConsoleException {

    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String tableName = DshipContext.INSTANCE.get(Constants.TABLE);

    Odps odps = OdpsConnectionFactory.createOdps(DshipContext.INSTANCE.getExecutionContext());
    TableTunnel tunnel = new TableTunnel(odps);

    if (tableProject == null) {
      tableProject = odps.getDefaultProject();
    }

    if (ps == null) {
      download = tunnel.createDownloadSession(tableProject, tableName);
    } else {
      download = tunnel.createDownloadSession(tableProject, tableName, ps);
    }

    totalLines = download.getRecordCount();
  }

  public DshipRecordReader getRecordReader(Long start, Long end) throws IOException, TunnelException {
    return new DshipRecordReader(this.download, start, end);
  }


  public TableSchema getSchema() {
    return download.getSchema();
  }

  public String getDownloadId() {
    return download.getId();
  }

  public long getTotalLines() {
    return totalLines;
  }
}

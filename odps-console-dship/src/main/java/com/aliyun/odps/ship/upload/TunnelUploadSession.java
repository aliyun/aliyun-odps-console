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

package com.aliyun.odps.ship.upload;

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
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class TunnelUploadSession implements TunnelUpdateSession{

  UploadSession upload;

  //just for test
  protected TunnelUploadSession(String str){
  }

  public TunnelUploadSession() throws OdpsException, IOException, ODPSConsoleException {
    String tableProject = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String schemaName = DshipContext.INSTANCE.get(Constants.SCHEMA);
    String tableName = DshipContext.INSTANCE.get(Constants.TABLE);
    String partitionSpec = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);

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

    if (StringUtils.isNotEmpty(DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID))) {
      if (ps == null) {
        upload = tunnel.getUploadSession(tableProject, schemaName, tableName,
                                         DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID));
      } else {
        upload = tunnel.getUploadSession(tableProject, schemaName, tableName, ps,
                                         DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID));
      }
    } else {
      boolean overwrite = Boolean.parseBoolean(DshipContext.INSTANCE.get(Constants.OVERWRITE));
      if (ps == null) {
        upload = tunnel.createUploadSession(tableProject, schemaName, tableName, overwrite);
      } else {
        if ("true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.AUTO_CREATE_PARTITION))) {
          Table t = odps.tables().get(tableProject, schemaName, tableName);
          if (!t.hasPartition(ps)) {
            System.err.println("Create partition " + ps.toString());
            t.createPartition(ps);
          }
        }
        upload = tunnel.createUploadSession(tableProject, schemaName, tableName, ps, overwrite);
      }
    }
    System.err.println("Upload session: " + upload.getId());
  }

  public void setScan(boolean isScan) {
    DshipContext.INSTANCE.put(Constants.SCAN, String.valueOf(isScan));
  }

  public boolean isScan() {
    return Boolean.valueOf(DshipContext.INSTANCE.get(Constants.SCAN));
  }

  public TableSchema getSchema() {
    return upload.getSchema();
  }


  @Override
  public Record getRecord(RecordConverter recordConverter, byte[][] textRecord)
      throws UnsupportedEncodingException, ParseException {
    return recordConverter.parse(textRecord);
  }

  @Override
  public void initRecord() {
    //do nothing
  }

  public RecordWriter getWriter(long bId) throws TunnelException, IOException {

    if (isScan()) {
      return new ScanerWriter();
    }
    return upload.openRecordWriter(bId, Boolean.valueOf(
        DshipContext.INSTANCE.get(Constants.COMPRESS)));
  }

  @Override
  public String getSessionId() {
    return upload.getId();
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.upload;
  }

  public void complete(List<Long> bList) throws TunnelException, IOException {

    int retry = 1;
    while (true) {
      try {
        upload.commit(bList.toArray(new Long[bList.size()]));
        break;
      } catch (TunnelException e) {
        System.err.println("Exception occurred while commit upload session: " + e.getMessage()
                           + ", retry(" + retry + "/" + Constants.RETRY_LIMIT + ")");
        retry++;
        if (retry > Constants.RETRY_LIMIT) {
          throw e;
        }
      } catch (IOException e) {
        System.err.println("Exception occurred while commit upload session: " + e.getMessage()
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
  }

//  public void abort() throws TunnelException, IOException {
//    upload.abort();
//  }

  class ScanerWriter implements RecordWriter {

    @Override
    public void close() throws IOException {
      // do nothing
    }

    @Override
    public void write(Record arg0) throws IOException {
      // do nothing
    }
  }
}

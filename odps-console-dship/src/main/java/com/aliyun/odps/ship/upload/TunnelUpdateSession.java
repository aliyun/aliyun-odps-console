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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.ship.common.CommandType;
import com.aliyun.odps.ship.common.RecordConverter;
import com.aliyun.odps.tunnel.TunnelException;

public interface TunnelUpdateSession {

  TableSchema getSchema();

  String getSessionId();

  CommandType getCommandType();

  RecordWriter getWriter(long bId) throws TunnelException, IOException;

  Record getRecord(RecordConverter recordConverter, byte[][] textRecord)
      throws UnsupportedEncodingException, ParseException;

  void initRecord();

  void complete(List<Long> bList) throws TunnelException, IOException;

  default boolean isScan(){
    return false;
  }

  default void setScan(boolean scan) {

  }
}

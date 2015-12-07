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

package com.aliyun.odps.ship.common;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by nizheming on 15/6/9.
 */
public class OptionsBuilderTest {
  @Before
  public void setup() throws ODPSConsoleException {
    DshipContext.INSTANCE.setExecutionContext(ExecutionContext.init());
  }

  @Test
  public void testCheckParameters() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", "table"});
    DshipContext.INSTANCE.put(Constants.THREADS, "1");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg1() throws Exception {
    OptionsBuilder.buildUploadOption(new String[]{"upload", "src/test/resources/many_record.txt", "table"});
    DshipContext.INSTANCE.put(Constants.THREADS, "-1");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg2() throws Exception {
    OptionsBuilder.buildUploadOption(new String[]{"upload", "src/test/resources/many_record.txt", "table"});
    DshipContext.INSTANCE.put(Constants.THREADS, "1.5");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg3() throws Exception {
    OptionsBuilder.buildUploadOption(new String[]{"upload", "src/test/resources/many_record.txt", "table"});
    DshipContext.INSTANCE.put(Constants.THREADS, "0");
    OptionsBuilder.checkParameters("upload");
  }
}
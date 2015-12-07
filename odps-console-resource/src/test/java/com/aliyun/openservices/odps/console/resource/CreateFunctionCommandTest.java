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

package com.aliyun.openservices.odps.console.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/8/25.
 */
public class CreateFunctionCommandTest {

  public static final String FUNCTION_UT_RESOURCE_ZIP = "function_ut_resource.zip";
  public static final String FUNCTION_UT_UPDATE_RESOURCE_ZIP = "function_ut_update_resource.zip";
  public static final String FUNCTION_UT_TEST = "function_ut_test";
  private static ExecutionContext init;
  private static Odps odps;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException, FileNotFoundException {
    init = ExecutionContext.init();
    odps = OdpsConnectionFactory.createOdps(init);
    String resource = CreateFunctionCommandTest.class.getResource("/resource.zip").getFile().toString();
    FileResource fileResource = new FileResource();
    fileResource.setName(FUNCTION_UT_RESOURCE_ZIP);
    if (odps.resources().exists(FUNCTION_UT_RESOURCE_ZIP)) {
      odps.resources().delete(FUNCTION_UT_RESOURCE_ZIP);
    }
    odps.resources().create(fileResource, new FileInputStream(resource));
    fileResource.setName(FUNCTION_UT_UPDATE_RESOURCE_ZIP);
    if (odps.resources().exists(FUNCTION_UT_UPDATE_RESOURCE_ZIP)) {
      odps.resources().delete(FUNCTION_UT_UPDATE_RESOURCE_ZIP);
    }
    odps.resources().create(fileResource, new FileInputStream(resource));

    if (odps.functions().exists(FUNCTION_UT_TEST)) {
      odps.functions().delete(FUNCTION_UT_TEST);
    }
  }

  @Test
  public void testCreateAndUpdateFunction() throws ODPSConsoleException, OdpsException {
    CreateFunctionCommand command = CreateFunctionCommand.parse("create function " + FUNCTION_UT_TEST + " as 'class.path' using '" + FUNCTION_UT_RESOURCE_ZIP + "'", init);
    command.run();
    assertTrue(odps.functions().exists(FUNCTION_UT_TEST));
    ;
    Function function = odps.functions().get(FUNCTION_UT_TEST);
    assertEquals(function.getName(), FUNCTION_UT_TEST);
    assertEquals(function.getClassPath(), "class.path");
    assertTrue(function.getResources().get(0).getName().contains(FUNCTION_UT_RESOURCE_ZIP));
    command = CreateFunctionCommand.parse("create function " + FUNCTION_UT_TEST + " as 'class_update.path' using '" + FUNCTION_UT_UPDATE_RESOURCE_ZIP + "' -f", init);
    command.run();
    function.reload();
    assertEquals(function.getClassPath(), "class_update.path");
    assertTrue(function.getResources().get(0).getName().contains(FUNCTION_UT_UPDATE_RESOURCE_ZIP));
  }


  @AfterClass
  public static void cleanup() throws OdpsException {
    odps.functions().delete(FUNCTION_UT_TEST);
    odps.resources().delete(FUNCTION_UT_RESOURCE_ZIP);
    odps.resources().delete(FUNCTION_UT_UPDATE_RESOURCE_ZIP);
  }
}
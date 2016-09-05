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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Volume;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.tunnel.VolumeTunnel;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class AddResourceCommandTest {

  public static String VOLUME_NAME = "test_volume";
  private static Odps odps;

  @BeforeClass
  public static void setUp() throws ODPSConsoleException, OdpsException {
    odps = OdpsConnectionFactory.createOdps(ExecutionContext.init());

    if (!odps.volumes().exists(VOLUME_NAME)) {
      odps.volumes().create(VOLUME_NAME, "test volume", Volume.Type.OLD);
    }
  }

  @Test
  public void testIsSecurityCommand() {
    assertTrue(AddResourceCommand.isSecurityCommand("add table t1 to package testpack"));
    assertTrue(AddResourceCommand
        .isSecurityCommand("add table t1 to package testpack WITH PRIVILEGES privileges"));
    assertTrue(AddResourceCommand.isSecurityCommand("\t\nadD tablE t1 tO  pAckage testpack;\t\n"));
    assertFalse(AddResourceCommand
        .isSecurityCommand("\t\nadD \rtablE t1 tO  pAckage testpack;\t\n"));
  }

  @Test
  public void testAddResourceCommandParser() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add archive file.zip /home/admin/alisatasknode/taskinfo//20150421/phoenix/20150420/1638166/11-09-38/yiixo5qwftxmbfqs3jdv52hx//main_sub0.zip";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
  }

  @Test
  public void testAddResourceVolumeArchive() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add volumearchive /volumename/a/b.zip as file.zip";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
  }

  @Test
  public void testAddResourceVolumeArchiveWithForce() throws ODPSConsoleException {
    String volumeFileName = "test_volume_file";
    ExecutionContext context = ExecutionContext.init();

    String commandText = null;
    AddResourceCommand command;

    commandText =
        "add volumefile /" + VOLUME_NAME + "/filepartition/testfile as " + volumeFileName + " -f";
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);

    // drop the resource if any
    try {
      command.getCurrentOdps().resources().delete(volumeFileName);
    } catch (OdpsException e) {
    }

    try {
      command.run();
    } catch (OdpsException e) {
      assertTrue(e.getMessage(), false);
    }
  }

  @Test(expected = ODPSConsoleException.class)
  public void testAddResourceVolumeArchiveNeg() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add volumearchive /volumename/a/b.a as b.a ";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testAddResourceVolueArchiveNeg2() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add volumeblabla b.a as  b.a";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNull(command);
  }

  @Test
  public void testAddResourceJarNotCheck() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add jar a as a.jar";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
  }


  @Test(expected = ODPSConsoleException.class)
  public void testAddResourceJarNotCheckNeg() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add jar a as a.b";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testAddResourceJarNotCheckNeg1() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add jar a";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

  @Test
  public void testAddResourcePyNotCheck() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add py a as a.py";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testAddResourcePyNotCheckNeg() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add py a as a.b";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testAddResourcePyNotCheckNeg1() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add py a";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

  @Test
  public void testAddVolumeArchiveExecute() throws ODPSConsoleException, OdpsException, IOException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AbstractCommand command;

    Odps odps = OdpsConnectionFactory.createOdps(context);

    if (odps.resources().exists("volumearchive.zip")) {
      odps.resources().delete("volumearchive.zip");
    }

    if (!odps.volumes().exists("addvolumetest")) {
      odps.volumes().create("addvolumetest", null);
    }

    VolumeTunnel tunnel = new VolumeTunnel(odps);
    VolumeTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(), "addvolumetest", "addvolumetest");

    OutputStream os = session.openOutputStream("volumearchive.zip");
    os.write(IOUtils.readFully(this.getClass().getResourceAsStream("/resource.zip")));
    os.close();

    System.out.println(StringUtils.join(session.getFileList()));
    session.commit(session.getFileList());

    commandText = String.format("add volumearchive /addvolumetest/addvolumetest/volumearchive.zip as volumearchive.zip");

    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
    command.run();

    command = ListResourcesCommand.parse("list resources", context);
    command.run();

    command = DescribeResourceCommand.parse("desc resource volumearchive.zip", context);
    command.run();

    commandText = String.format("add volumearchive /addvolumetest/addvolumetest/volumearchive.zip as volumearchive.zip -f");

    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testArchiveQuoate() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    AddResourceCommand command;
    String resource = this.getClass().getResource("/resource.zip").getFile().toString();
    String commandText = "add archive " + resource + " as \"a.zip\" -f";
    // A warning will be output to console
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

  private static final String CONSOLE_USER_PATH = "console_user_path";

  @Test
  public void testAddHomeResource() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    try {
      FileUtils.touch(new File(System.getProperty("user.home") + "/" + CONSOLE_USER_PATH));
    } catch (IOException e) {
      e.printStackTrace();
    }
    String commandText = null;
    AddResourceCommand command;

    commandText = "add file ~/" + CONSOLE_USER_PATH + " -f";
    command = AddResourceCommand.parse(commandText, context);
    command.run();

    FileUtils.deleteQuietly(new File(System.getProperty("user.home") + "/" + CONSOLE_USER_PATH));
  }

  @Test(expected = ODPSConsoleException.class)
  public void testAddHomeNeg() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add file ~a";
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }


  @Test(expected = ODPSConsoleException.class)
  public void testAddHomeNeg1() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "add file a~";
    command = AddResourceCommand.parse(commandText, context);
    command.run();
  }

}

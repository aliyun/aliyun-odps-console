package com.aliyun.openservices.odps.console.commands;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * Created by zhenhong.gzh on 16/1/14.
 */
public class ExecuteScriptCommandTest {
  @Test
  public void testPositive() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    ExecuteScriptCommand command = null;

    List<String> options = new ArrayList<String>();
    options.add("-s");

    options.add(ODPSConsoleUtils.getConfigFilePath());

    command = ExecuteScriptCommand.parse(options, context);
    Assert.assertNotNull(command);

    Assert.assertEquals(ODPSConsoleUtils.getConfigFilePath(), command.getFilename());
  }

  @Test
  public void testNegative() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    ExecuteScriptCommand command = null;

    List<String> options = new ArrayList<String>();
    options.add("-f");

    options.add(ODPSConsoleUtils.getConfigFilePath());

    command = ExecuteScriptCommand.parse(options, context);
    Assert.assertNull(command);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testFileNotExsit() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    ExecuteScriptCommand command = null;

    List<String> options = new ArrayList<String>();
    options.add("-s");

    options.add("test_file");

    ExecuteScriptCommand.parse(options, context);
  }

}

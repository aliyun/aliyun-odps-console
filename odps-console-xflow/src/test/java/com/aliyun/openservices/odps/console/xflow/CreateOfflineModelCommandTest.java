package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNotNull;

public class CreateOfflineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testCreateOfflineModelWithType() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create offlinemodel t_offlinemodel " +
            "-modelPath oss://bucket/dir/?role_arn=xxx&host=yyy " +
            "-type tensorflow " +
            "-version 1.0 " +
            "-configuration '{\"path\":\"abc/\"}'";
    CreateOfflineModelCommand command = CreateOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testCreateOfflineModelWithProcessor() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create offlinemodel t_offlinemodel " +
            "-modelPath oss://bucket/dir/?role_arn=xxx&host=yyy " +
            "-processor '[{\"id\":\"MnistProcessor\",\"libName\":\"libmnist_processor.so\"," +
            "\"refResource\":\"mnist_processor.tar.gz\",\"configuration\":\"\",\"runtime\":\"Native\"}]'";
    CreateOfflineModelCommand command = CreateOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testCreateOffLineModelWithBoth() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create offlinemodel t_offlinemodel " +
            "-modelPath oss://bucket/dir/?role_arn=xxx&host=yyy " +
            "-processor '[{\"id\":\"MnistProcessor\",\"libName\":\"libmnist_processor.so\"," +
            "\"refResource\":\"mnist_processor.tar.gz\",\"configuration\":\"\",\"runtime\":\"Native\"}]' " +
            "-type tensorflow " +
            "-version 1.0";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND +
            "invalid parameter for offlinemodel, please HELP OFFLINEMODEL.");
    CreateOfflineModelCommand command = CreateOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testCreateOffineAmbiguousModelName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create offlinemodel t_offlinemodel t_offlinemodel2 " +
            "-modelPath oss://bucket/dir/?role_arn=xxx&host=yyy " +
            "-type tensorflow " +
            "-version 1.0";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Model name is ambiguous.");
    CreateOfflineModelCommand command = CreateOfflineModelCommand.parse(cmd, context);
  }
}

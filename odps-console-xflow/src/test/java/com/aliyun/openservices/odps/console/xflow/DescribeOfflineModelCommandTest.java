package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

/**
 * Created by benjin.mbj on 16/3/16.
 */
public class DescribeOfflineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testDescribeOffLineModelMatchNone() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel";
    DescribeOfflineModelCommand command = DescribeOfflineModelCommand.parse(cmd, context);
    assertNull(command);
  }

  @Test
  public void testDescribeOffLineModelNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel model";
    DescribeOfflineModelCommand command = DescribeOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOffLineModelWithPrj() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel prj.model";
    DescribeOfflineModelCommand command = DescribeOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOffLineModelWithPrjOption() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel -p prj model";
    DescribeOfflineModelCommand command = DescribeOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOffLineModelConflictPrjName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel -p prj1 prj2.model";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Project name conflict.");
    DescribeOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testDescribeOffLineModelNoModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel -p prj ";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Model name not found.");
    DescribeOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testDescribeOffLineModelInvalidModelName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc offlinemodel model-error";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    DescribeOfflineModelCommand.parse(cmd, context);
  }
}

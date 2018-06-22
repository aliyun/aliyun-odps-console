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
public class DescribeOnlineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testDescribeOnLineModelMatchNone() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel";
    DescribeOnlineModelCommand command = DescribeOnlineModelCommand.parse(cmd, context);
    assertNull(command);
  }

  @Test
  public void testDescribeOnLineModelNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel xxx";
    DescribeOnlineModelCommand command = DescribeOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Onlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOnLineModelWithPrj() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel prj.model";
    DescribeOnlineModelCommand command = DescribeOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Onlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOnLineModelWithPrjOption() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel -p prj model";
    DescribeOnlineModelCommand command = DescribeOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Onlinemodel not found : ");
    command.run();
  }

  @Test
  public void testDescribeOnLineModelConflictPrjName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel -p prj1 prj2.model";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Project name conflict.");
    DescribeOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testDescribeOnLineModelNoModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel -p prj ";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Model name is ambiguous.");
    DescribeOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testDescribeOnLineModelInvalidModelName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc onlinemodel model-error";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    DescribeOnlineModelCommand.parse(cmd, context);
  }
}

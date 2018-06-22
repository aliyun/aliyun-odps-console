package com.aliyun.openservices.odps.console.xflow;

import static org.junit.Assert.*;

import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import org.junit.rules.ExpectedException;

/**
 * Created by nizheming on 16/2/4.
 */
public class ShowOnlineModelsCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testShowOnLineModelsNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show onlinemodels";
    ShowOnlineModelsCommand command = ShowOnlineModelsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowOnLineModelsWithFilter() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show onlinemodels prefix";
    ShowOnlineModelsCommand command = ShowOnlineModelsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowOnLineModelsInvalidFilter() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show onlinemodels prefix-error";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid model prefix.");
    ShowOnlineModelsCommand.parse(cmd, context);
  }

  @Test
  public void testShowOnLineModelsTooManyParams() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show onlinemodels a b";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    ShowOnlineModelsCommand.parse(cmd, context);
  }
}

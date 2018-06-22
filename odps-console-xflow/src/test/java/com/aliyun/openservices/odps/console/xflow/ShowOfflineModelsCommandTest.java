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
public class ShowOfflineModelsCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testShowOffLineModelsNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show offlinemodels";
    ShowOfflineModelsCommand command = ShowOfflineModelsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowOffLineModelsWithFilter() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show offlinemodels prefix";
    ShowOfflineModelsCommand command = ShowOfflineModelsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowOffLineModelsInvalidFilter() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show offlinemodels prefix-error";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid model prefix.");
    ShowOfflineModelsCommand.parse(cmd, context);
  }

  @Test
  public void testShowOffLineModelsTooManyParams() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show offlinemodels a b";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    ShowOfflineModelsCommand.parse(cmd, context);
  }
}

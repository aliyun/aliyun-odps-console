package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNotNull;

/**
 * Created by benjin.mbj on 16/11/23.
 */
public class ShowXflowsCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testShowXflowsNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show xflows";
    ShowXflowsCommand command = ShowXflowsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowXflowsWithProject() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show xflows -p algo_public";
    ShowXflowsCommand command = ShowXflowsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowXflowsWithOwner() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show xflows ALIYUN$odpstest1@aliyun.com";
    ShowXflowsCommand command = ShowXflowsCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testShowXflowsTooManyParams() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "show xflows a b";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    ShowXflowsCommand.parse(cmd, context);
  }
}

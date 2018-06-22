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
public class ReadOfflineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testReadOffLineModelMatchNone() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNull(command);
  }

  @Test
  public void testReadOffLineModelNormal() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel model";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testReadOffLineModelWithPrj() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel prj.model";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testReadOffLineModelWithPrjOption() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel -p prj model";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testReadOffLineModelConflitPrjName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel -p prj1 prj2.model";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Project name conflict.");
    ReadOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testReadOffLineModelNoModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel -p prj";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Model name not found.");
    ReadOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testReadOffLineModelInvalidModelName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel model-error";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid model name.");
    ReadOfflineModelCommand.parse(cmd, context);
  }

  @Test
  public void testReadOffLineModelWithVolume() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel model as volume";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testReadOffLineModelWithVolumePartition() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel model as volume.partition";
    ReadOfflineModelCommand command = ReadOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Offlinemodel not found : ");
    command.run();
  }

  @Test
  public void testReadOffLineModelWithAsStatementError() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "read offlinemodel model as_error volume";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    ReadOfflineModelCommand.parse(cmd, context);
  }
}

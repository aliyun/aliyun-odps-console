package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class UpdateOnlineModelAbtestCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testUpdateOnLineModelAbtest() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "updateabtest onlinemodel -p online_model_test t_offlinemodel " +
            "-targetProject online_model_test " +
            "-targetModel model1 " +
            "-percentage 10";
    UpdateOnlineModelAbtestCommand command = UpdateOnlineModelAbtestCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testUpdateOnLineModelAbtestClear() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "updateabtest onlinemodel -p online_model_test t_offlinemodel";
    UpdateOnlineModelAbtestCommand command = UpdateOnlineModelAbtestCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testUpdateAbtestOnLineModelNotFound() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "updateabtest onlinemodel -p online_model_test model_not_found " +
            "-targetProject online_model_test " +
            "-targetModel model1 " +
            "-percentage 10";
    UpdateOnlineModelAbtestCommand command = UpdateOnlineModelAbtestCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Onlinemodel not found: ");
    command.run();
  }

  @Test
  public void testUpdateAbtestOnLineModelParametetInvalid() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "updateabtest onlinemodel -p online_model_test model_not_found " +
            "-targetProject online_model_test " +
            "-percentage 10";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel abtest, please HELP ONLINEMODEL.");
    UpdateOnlineModelAbtestCommand command = UpdateOnlineModelAbtestCommand.parse(cmd, context);
    assertNull(command);
  }
}

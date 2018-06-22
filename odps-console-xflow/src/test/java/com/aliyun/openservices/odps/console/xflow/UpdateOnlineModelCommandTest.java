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

public class UpdateOnlineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testUpdateOnLineModelWithOfflineModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "update onlinemodel -p online_model_test t_offlinemodel " +
            "-offlinemodelProject online_model_test " +
            "-offlinemodelName lr_model_for_online " +
            "-qos 50 " +
            "-instanceNum 1 " +
            "-cpu 400 " +
            "-memory 3";
    UpdateOnlineModelCommand command = UpdateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testUpdateOnLineModelNotFound() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "update onlinemodel -p online_model_test model_not_found " +
            "-offlinemodelProject online_model_test " +
            "-offlinemodelName lr_model_for_online " +
            "-qos 50 " +
            "-instanceNum 1 " +
            "-cpu 400 " +
            "-memory 3";
    UpdateOnlineModelCommand command = UpdateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Onlinemodel not found: ");
    command.run();
  }

  @Test
  public void testUpdateOnLineModelParameterError1() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "update onlinemodel -p prj1 model1 " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    UpdateOnlineModelCommand command = UpdateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testUpdateOnLineModelParameterError2() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "update onlinemodel -p prj1 model1 " +
            "-target target1 " +
            "-libName lib1 " +
            "-refResource res1 " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    UpdateOnlineModelCommand command = UpdateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testUpdateOnLineModelParameterError3() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "update onlinemodel -p prj1 model1 " +
            "-target target1 " +
            "-id id1 " +
            "-libName lib1 " +
            "-refResource res1 " +
            "-offlinemodelName aaa " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    UpdateOnlineModelCommand command = UpdateOnlineModelCommand.parse(cmd, context);
  }
}

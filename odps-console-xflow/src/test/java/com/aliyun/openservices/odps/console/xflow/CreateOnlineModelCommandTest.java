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

public class CreateOnlineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testCreateOnLineModelWithOfflineModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel t_offlinemodel " +
            "-offlinemodelProject online_model_test " +
            "-offlinemodelName lr_model_for_online " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testCreateOnLineModelWith3rdModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel t_3rd_cpp " +
            "-target abc " +
            "-id SampleProcessor " +
            "-libName libsample_processor.so " +
            "-refResource online_model_test/resources/sample_processor.tar.gz " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-gpu 100 " +
            "-memory 2";
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testCreateOnLineModelWithJavaModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel t_3rd_jar " +
            "-target abc " +
            "-id com.aliyun.openservices.odps.predict.pmml.PmmlProcessor " +
            "-libName predict-pmml-processor.jar " +
            "-refResource online_model_test/resources/predict-pmml-processor.jar,online_model_test/resources/irislinearreg.xml " +
            "-configuration irislinearreg.xml " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2 " +
            "-runtime Jar";
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
  }

  @Test
  public void testCreateOnLineModelParameterError1() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel -p prj1 model1 " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testCreateOnLineModelParameterError2() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel -p prj1 model1 " +
            "-target target1 " +
            "-libName lib1 " +
            "-refResource res1 " +
            "-qos 5000 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-memory 2";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("invalid parameter for onlinemodel, please HELP ONLINEMODEL.");
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testCreateOnLineModelParameterError3() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel -p prj1 model1 " +
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
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testCreateOnLineModelParameterError4() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel -p prj1 model1 " +
            "-target target1 " +
            "-id id1 " +
            "-libName lib1 " +
            "-refResource res1 " +
            "-instanceNum 2 " +
            "-cpu 100 " +
            "-runtime xxx";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage("Parameter -runtime must be Jar or Native.");
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
  }

  @Test
  public void testCreateOnLineModelWithoutPrj() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel model " +
            "-offlinemodelName lr_model_for_online";
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    expectEx.expectMessage("model not found - ");
    command.run();
  }

  @Test
  public void testCreateOnLineModelExists() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "create onlinemodel model_exist " +
            "-offlinemodelProject online_model_test " +
            "-offlinemodelName lr_model_for_online";
    CreateOnlineModelCommand command = CreateOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    //command.run();
    //expectEx.expect(ODPSConsoleException.class);
    //expectEx.expectMessage("Onlinemodel already exists : ");
    //command.run();
  }

}

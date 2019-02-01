package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertNotNull;

public class CopyOfflineModelCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testCopyOfflineModelBasic() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "copy offlinemodel " +
            "-src_model model1 " +
            "-dest_model model2 " +
            "-src_project project1 " +
            "-dest_project project2";
    CopyOfflineModelCommand command = CopyOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testCopyOfflineModelOmitProjectName() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "copy offlinemodel " +
        "-src_model model1 " +
        "-dest_model model2 ";
    CopyOfflineModelCommand command = CopyOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
  }

  @Test
  public void testCopyOfflineModelInvalidCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "copy offlinemodel dummy " +
        "-src_model model1 " +
        "-dest_model model2 ";
    CopyOfflineModelCommand command = CopyOfflineModelCommand.parse(cmd, context);
    assertNotNull(command);
  }
}

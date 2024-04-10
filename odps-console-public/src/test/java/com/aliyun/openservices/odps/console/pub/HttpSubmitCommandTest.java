package com.aliyun.openservices.odps.console.pub;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by zhenhong on 15/12/3.
 */
public class HttpSubmitCommandTest {

  private static String[]
      positives =
      {"http put \r /projects/*",
       "HTTP get /projects/test_project \n-header=headers.file",
       "http get /project/test \t\r\n -header=header.file \n-content=content.file"};

  private static String[]
      negatives =
      {"HTTP putt /projects/test_project",
       "HTTP put /projects/test_project test",
       "http get /projects/test_project -wrong=headers.file",
       "http get /project/test -header=header.file -content=content.file test"};

  @Test
  public void testHttpSubmitCommandPositive() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : positives) {
      Assert.assertNotNull(HttpSubmitCommand.parse(cmd, ctx));
    }
  }

  @Test
  public void testHttpSubmitCommandNegative() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    int count = 0;
    for (String cmd : negatives) {
      try {
        HttpSubmitCommand.parse(cmd, ctx);
      } catch (ODPSConsoleException e) {
        count++;
      }
    }

    Assert.assertEquals(negatives.length, count);
  }

  @Test
  public void testHttpSubmitCommand() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    String cmd = "http get /projects/*".replace("*", ctx.getProjectName());
    HttpSubmitCommand command = HttpSubmitCommand.parse(cmd, ctx);
    command.run();
  }
}

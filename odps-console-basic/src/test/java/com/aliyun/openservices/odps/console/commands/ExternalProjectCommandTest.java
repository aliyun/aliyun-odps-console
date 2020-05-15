package com.aliyun.openservices.odps.console.commands;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExternalProjectCommandTest {
  private static String[] createCommands = {
          "crEate ExternalProject -name test1 -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
          "create ExternalProject -name test1 -comment test -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                  " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"",
  };

  private static String[] updateCommands = {
          "UPDATE ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
          "upDate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                  " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"",
  };

  private static String[] deleteCommands = {
          "delete Externalproject -name p1",
          "dElete externalProject -name p2"
  };

  // Commands will not be parsed as ExternalCommand
  private static String[] nullCommands = {
          "whatever ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
          "deletex ExternalProject"
  };

  // Commands that ARE external commands but have invalid parameter. Key is command text, and value is error message.
  private static Map<String,String> invalidCommands = new HashMap<String, String>() {{
    put("crEate ExternalProject -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default", "Missing required option: name");
    put("crEate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default", "ref is required");
    put("upDate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default -vpc vpc1", "region is required");
    put("upDate ExternalProject -name test1 -nn \"\" -hms \"1.1.1.1:8339\" -db default -vpc vpc1 -region cn-zhangjiakou", "nn is required and not allowed to be empty");
    put("upDate ExternalProject -name test1 -nn \"1.1.1.1:3833\" -hms \"1.1.1.1:8339\" -db default -vpc vpc1 -region cn-zhangjiakou", "accessIp is required and not allowed to be empty");
    put("delete ExternalProject -ref abc", "Missing required option: name");
  }};

  @Test
  public void testParseCommandPass() throws Exception {
    for (String cmd: createCommands) {
      ExecutionContext context = ExecutionContext.init();
      AbstractCommand command = ExternalProjectCommand.parse(cmd, context);

      Assert.assertTrue(command instanceof ExternalProjectCommand);
      ExternalProjectCommand extCommand = (ExternalProjectCommand)command;

      Assert.assertEquals(extCommand.getAction().getActionName(), "create");
    }

    for (String cmd: updateCommands) {
      ExecutionContext context = ExecutionContext.init();
      AbstractCommand command = ExternalProjectCommand.parse(cmd, context);

      Assert.assertTrue(command instanceof  ExternalProjectCommand);
      ExternalProjectCommand extCommand = (ExternalProjectCommand)command;

      Assert.assertEquals(extCommand.getAction().getActionName(), "update");
    }

    for (String cmd: deleteCommands) {
      ExecutionContext context = ExecutionContext.init();
      AbstractCommand command = ExternalProjectCommand.parse(cmd, context);

      Assert.assertTrue(command instanceof ExternalProjectCommand);
      ExternalProjectCommand extCommand = (ExternalProjectCommand)command;

      Assert.assertEquals(extCommand.getAction().getActionName(), "delete");
    }
  }

  @Test
  public void testNotExternalCommand() throws Exception {
    for (String cmd: nullCommands) {
      Assert.assertNull(ExternalProjectCommand.parse(cmd, ExecutionContext.init()));
    }
  }

  @Test
  public void testInvalidCommand() {
    for (Map.Entry<String,String> cmd: invalidCommands.entrySet()) {
      try {
        ExternalProjectCommand.parse(cmd.getKey(), ExecutionContext.init());
        Assert.fail("Exception expected for command: " + cmd.getKey());
      } catch (IllegalArgumentException err) {
        // IllegalArgumentExceptions are thrown by Action.validParams
        Assert.assertTrue(err.getMessage().contains(cmd.getValue()));
      } catch (ODPSConsoleException conErr) {
        // name is a required Option and will fail at Parsing phase of CommandLineParse
        ParseException parseErr = (ParseException)(conErr.getCause());
        Assert.assertTrue(parseErr.getMessage().contains(cmd.getValue()));
      }
    }
  }
}

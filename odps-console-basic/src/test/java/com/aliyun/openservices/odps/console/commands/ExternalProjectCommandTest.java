package com.aliyun.openservices.odps.console.commands;

import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExternalProjectCommandTest {
  // key:value = cmd: expected external properties json
  private static final Map<String, String> createCommands = new HashMap<String, String>() {{
    put("crEate ExternalProject -name test1 -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
            "{\"source\":\"hive\", \"hms.ips\": \"1.1.1.1:8339\", \"hive.database.name\": \"default\"," +
                    " \"hdfs.namenode.ips\":\"11.12.13.14:3335,12.33.33.45:3829\",\"network\":{\"odps.external.net.vpc\":\"false\"}}");
    put("create ExternalProject -name test1 -comment test -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                    " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"",
            "{\"source\":\"hive\", \"hms.ips\": \"1.1.1.1:8339\", \"hive.database.name\": \"default\"," +
                    " \"hdfs.namenode.ips\":\"11.12.13.14:3335,12.33.33.45:3829\"," +
                    "\"network\":{\"odps.external.net.vpc\":\"true\", \"odps.vpc.region\":\"cn-shanghai\"," +
                    "\"odps.vpc.id\":\"vpc1\", \"odps.vpc.access.ips\":\"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"}}");
    put("create ExternalProject -name test1 -comment test -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                    " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"" +
                    " -hmsPrincipals \"principal1,principal2\" -D dfs.data.transfer.protection=integrity",
            "{\"source\":\"hive\", \"hms.ips\": \"1.1.1.1:8339\", \"hive.database.name\": \"default\"," +
                    " \"hdfs.namenode.ips\":\"11.12.13.14:3335,12.33.33.45:3829\"," +
                    " \"hms.principals\":\"principal1,principal2\"," +
                    "\"network\":{\"odps.external.net.vpc\":\"true\", \"odps.vpc.region\":\"cn-shanghai\"," +
                    "\"odps.vpc.id\":\"vpc1\", \"odps.vpc.access.ips\":\"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\", \"dfs.data.transfer.protection\":\"integrity\"}}");
    put("create ExternalProject -name test1 -comment test -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
            " -db default -vpc vpc1 -region cn-shanghai",
            "{\"source\":\"hive\", \"hms.ips\": \"1.1.1.1:8339\", \"hive.database.name\": \"default\"," +
                    " \"hdfs.namenode.ips\":\"11.12.13.14:3335,12.33.33.45:3829\"," +
                    "\"network\":{\"odps.external.net.vpc\":\"true\", \"odps.vpc.region\":\"cn-shanghai\"," +
                    "\"odps.vpc.id\":\"vpc1\", \"odps.vpc.access.ips\":\"11.12.13.14:3335,12.33.33.45:3829\"}}");
    put("create ExternalProject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default -endpoint testxxx",
            "{\"source\":\"dlf\", \"dlf.region\": \"cn-shanghai\", \"dlf.endpoint\": \"testxxx\", \"dlf.database.name\": \"default\", \"network\":{}}");
    put("create ExternalProject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default -endpoint testxxx " +
                    "-ramRoleArn \"acs:ram::12345:role/myrolefordlfonodps\"",
            "{\"source\":\"dlf\", \"dlf.region\": \"cn-shanghai\", \"dlf.endpoint\": \"testxxx\", " +
                    "\"dlf.rolearn\": \"acs:ram::12345:role/myrolefordlfonodps\", \"dlf.database.name\": \"default\", \"network\":{}}");
    put("create ExternalProject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default -endpoint testxxx " +
                    "-ramRoleArn \"acs:ram::12345:role/myrolefordlfonodps\" -ossEndpoint \"test.oss.endpoint\" -T file_format=\"orc\" -T output_format=text",
            "{\"source\":\"dlf\", \"dlf.region\": \"cn-shanghai\", \"dlf.endpoint\": \"testxxx\", " +
                    "\"oss.endpoint\":\"test.oss.endpoint\", \"table_properties\": {\"file_format\": \"orc\", \"output_format\":\"text\"}, " +
                    "\"dlf.rolearn\": \"acs:ram::12345:role/myrolefordlfonodps\", \"dlf.database.name\": \"default\", \"network\":{}}");
  }};

  private static final String[] updateCommands = {
          "UPDATE ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
          "upDate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                  " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"",
          "upDate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
                  " -db default -vpc vpc1 -region cn-shanghai",
          "UPDATE ExternalProject -source dlf -name p1 -region cn-shanghai -db default1 -endpoint testxxx;",
          "upDate ExternalProject -source dlf -name p1 -comment test -region cn-shanghai -db default -endpoint testxxx " +
                  "-ramRoleArn \"acs:ram::123433995:role/myrolefordlfonodps\";"
  };

  private static final String[] deleteCommands = {
          "delete Externalproject -name p1",
          "dElete externalProject -name p2"
  };

  // Commands will not be parsed as ExternalCommand
  private static final String[] nullCommands = {
          "whatever ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default",
          "deletex ExternalProject"
  };

  // Commands that ARE external commands but have invalid parameter. Key is command text, and value is error message.
  private static final Map<String,String> invalidCommands = new HashMap<String, String>() {{
    put("crEate ExternalProject -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default", "Missing required option: name");
    put("create ExternalProject -source dlf -ref myprj1 -comment test -db default -region cn-xxx -endpoint testxxx;",  "Missing required option: name");
    put("create ExternalProject -source dlf -name p1 -ref myprj1 -comment test -db default -endpoint testxxx;",  "region is required");
    put("create ExternalProject -source dlf -name p1 -ref myprj1 -comment test -db default -region cn-shanghai;",  "endpoint is required");
    put("upDate ExternalProject -name test1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\" -db default -vpc vpc1", "region is required");
    put("upDate ExternalProject -name test1 -nn \"\" -hms \"1.1.1.1:8339\" -db default -vpc vpc1 -region cn-zhangjiakou", "nn is required and not allowed to be empty");
    put("update ExternalProject -source dlf -db default -region cn-xxx -endpoint testxxx;",  "Missing required option: name");
    put("update ExternalProject -source dlf -name p1 -ref myprj1 -region cn-xxx -endpoint testxxx;",  "db is required");
    put("delete ExternalProject -ref abc", "Missing required option: name");
  }};

  @Test
  public void testParseCommandPass() throws Exception {
    for (Map.Entry<String,String> test: createCommands.entrySet()) {
      ExecutionContext context = ExecutionContext.init();
      AbstractCommand command = ExternalProjectCommand.parse(test.getKey(), context);

      Assert.assertTrue(command instanceof ExternalProjectCommand);
      ExternalProjectCommand extCommand = (ExternalProjectCommand)command;
      ExternalProjectCommand.Action action = extCommand.getAction();
      Assert.assertEquals("create", action.getActionName());

      Project.ExternalProjectProperties externalProjectProperties = action.finalExternalProjectProperties();

      Gson gson = new Gson();
      JsonObject parsed = gson.fromJson(externalProjectProperties.toJson(), JsonObject.class);
      JsonObject expected = gson.fromJson(test.getValue(), JsonObject.class);
      Assert.assertEquals(expected, parsed);
    }

    // with properties
    {
      ExecutionContext context = ExecutionContext.init();
      String withProperties = "create ExternalProject -name test1 -comment test -ref ref1 -nn \"11.12.13.14:3335,12.33.33.45:3829\" -hms \"1.1.1.1:8339\"" +
              " -db default -vpc vpc1 -region cn-shanghai -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"" +
              " -Dodps.properties.rolearn=abcde" +
              " -D odps.properties.oss.endpoint=http://cn-hangzhou-internal.aliyuncs.com" +
              " -D spaced=\"abc def\" -D space_and_quote='\"abc def' -D space_and_apostrophe=\"'def def\"";
      AbstractCommand command = ExternalProjectCommand.parse(withProperties, context);

      Assert.assertTrue(command instanceof ExternalProjectCommand);
      ExternalProjectCommand extCommand = (ExternalProjectCommand)command;

      Project.ExternalProjectProperties externalProjectProperties = extCommand.getAction().finalExternalProjectProperties();
      Assert.assertEquals(extCommand.getAction().getActionName(), "create");

      Gson gson = new Gson();
      JsonObject parsed = gson.fromJson(externalProjectProperties.toJson(), JsonObject.class);
      JsonObject networkObj = parsed.getAsJsonObject("network");
      Assert.assertEquals(
              "abcde",
              networkObj.get("odps.properties.rolearn").getAsString()
      );

      Assert.assertEquals(
              "http://cn-hangzhou-internal.aliyuncs.com",
              networkObj.get("odps.properties.oss.endpoint").getAsString()
      );

      Assert.assertEquals(
              "\"abc def",
              networkObj.get("space_and_quote").getAsString()
      );

      Assert.assertEquals(
              "'def def",
              networkObj.get("space_and_apostrophe").getAsString()
      );
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

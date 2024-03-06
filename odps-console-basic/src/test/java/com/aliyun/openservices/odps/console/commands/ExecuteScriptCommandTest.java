package com.aliyun.openservices.odps.console.commands;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by zhenhong.gzh on 16/1/14.
 */
public class ExecuteScriptCommandTest {

  private static final String TEST_SCRIPT_FILE = ODPSConsoleUtils.getResourceFilePath("test_sql_script.sql");
  private static final String FILE_NOT_EXIST = "file_not_exist";

  @Test
  public void testParsePositive() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    List<String> options = Lists.newArrayList("-s", TEST_SCRIPT_FILE);
    ExecuteScriptCommand command = ExecuteScriptCommand.parse(options, context);
    Assert.assertNotNull(command);
    Assert.assertEquals(TEST_SCRIPT_FILE, command.filename);
  }

  @Test
  public void testParseNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    List<String> options = Lists.newArrayList("-f", TEST_SCRIPT_FILE);
    ExecuteScriptCommand command = ExecuteScriptCommand.parse(options, context);
    Assert.assertNull(command);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testFileNotExist() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    List<String> options = Lists.newArrayList("-s", FILE_NOT_EXIST);
    ExecuteScriptCommand.parse(options, context);
  }

  @Test
  public void testParseSettings() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    List<String> options = Lists.newArrayList("-s", TEST_SCRIPT_FILE);
    ExecuteScriptCommand command = ExecuteScriptCommand.parse(options, context);
    Assert.assertNotNull(command);
    command.parseSettings();
    Assert.assertEquals("a", context.getRunningCluster());
    Assert.assertEquals("Asia/Shanghai", context.getSqlTimezone());
    Assert.assertEquals("b", SetCommand.setMap.get("a"));
    Assert.assertEquals("c", SetCommand.setMap.get("b"));
  }

  @Test
  public void testComment() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String[] testCase = {
            "set a=b;\n-- comment\n",
            "set a=b;\n-- comment", "\n-- comment",
            "set a=b;-- comment", "-- comment",
            "set a=b;\n-- comment\nselect 1;", "\n-- comment\nselect 1;",
            "set a=b;\n-- comment\nselect 1;\n", "\n-- comment\nselect 1;\n",
            "-- abc\nset a=b;\n-- bc\nselect 1;--abc\n--ab\n"
    };

    for (String testScript : testCase) {
      ExecuteScriptCommand command = new ExecuteScriptCommand(testScript, context, "mock_file");
      Assert.assertNotNull(command);
      command.parseSettings();
      Assert.assertEquals(testScript, command.getCommandText());
      Assert.assertEquals("b", SetCommand.setMap.get("a"));
    }

  }

  @After
  public void clean() {
    SetCommand.setMap.clear();
  }
}

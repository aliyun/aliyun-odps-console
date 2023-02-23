package com.aliyun.openservices.odps.console.commands;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import junit.framework.TestCase;

public class ShowVersionCommandTest extends TestCase {

  private static String[] positive = {
      "show version",
      "SHOW VERSION",
      " sHow\n\r\tversion\t"
  };

  private static String[] negative = {
      "showversion"
  };

  @Test
  public void testPositive() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : positive) {
      Assert.assertNotNull(ShowVersionCommand.parse(cmd, context));
    }
  }

  @Test
  public void testNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : negative) {
      Assert.assertNull(ShowVersionCommand.parse(cmd, context));
    }
  }

}
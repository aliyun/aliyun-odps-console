package com.aliyun.openservices.odps.console.pub;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

public class TriggerAutoMvCommandTest {

  private static final String[] positives = {
      "TRIGGER AUTOMVCREATION",
      "TRIGGER automvcreation",
      "trigger AUTOMVCREATION"
  };

  private static final String[] not_match_negatives = {
      "TRIGGER",
      "TRIGGER AUTOMVCREATION a",
      "AUTOMVCREATION",
      };


  @Test
  public void testCommandParse() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : positives) {
      AbstractCommand command = TriggerAutoMvCommand.parse(cmd, context);
      assertNotNull(command);
    }
  }

  @Test
  public void testCommandNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : not_match_negatives) {
      AbstractCommand command = TriggerAutoMvCommand.parse(cmd, context);
      assertNull(command);
    }
  }

}
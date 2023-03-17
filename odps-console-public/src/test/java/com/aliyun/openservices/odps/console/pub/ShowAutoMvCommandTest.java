package com.aliyun.openservices.odps.console.pub;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

public class ShowAutoMvCommandTest {

  private static final String[] positives = {
      "show AUTOMVMETA",
      "SHOW automvmeta"
  };

  private static final String[] not_match_negatives = {
      "SHOW",
      "show automvmeta a",
      "automvmeta",
      };


  @Test
  public void testCommandParse() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : positives) {
      AbstractCommand command = ShowAutoMvCommand.parse(cmd, context);
      assertNotNull(command);
    }
  }

  @Test
  public void testCommandNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : not_match_negatives) {
      AbstractCommand command = ShowAutoMvCommand.parse(cmd, context);
      assertNull(command);
    }
  }

}
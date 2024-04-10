package com.aliyun.openservices.odps.console.commands;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;

/**
 * Created by zhenhong.gzh on 16/3/17.
 */
public class HistoryCommandTest {
  private String [] positive = {"history", " HistoRY", "HISTORY", "\t\rhistory \n"};

  private String [] negative = {"histry", " HisoRY", "HIS\t\rTORY"};

  @Test
  public void test() throws Exception {
    HistoryCommand historyCommand = null;
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : positive) {
      historyCommand = HistoryCommand.parse(cmd, context);
      assertNotNull(historyCommand);
    }

    for (String cmd : negative) {
      historyCommand = HistoryCommand.parse(cmd, context);
      assertNull(historyCommand);
    }
  }
}

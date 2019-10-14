package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import org.jline.reader.History;

/**
 * Created by zhenhong.gzh on 16/3/17.
 */
public class HistoryCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"history"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: history");
  }

  public void run() throws OdpsException, ODPSConsoleException {
    if (ODPSConsoleUtils.isWindows()) {
      getContext().getOutputWriter().writeError("Not supported on Windows");
      return;
    }

    History history = ODPSConsoleUtils.getOdpsConsoleReader().getHistory();
    if (history != null) {
      for (History.Entry entry : history) {
        getContext().getOutputWriter()
            .writeError(String.valueOf(entry.index()) + ' ' + entry.line());
      }
    }
  }

  public HistoryCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public static HistoryCommand parse(String cmd, ExecutionContext cxt) {
    if (cmd == null || cxt == null || ODPSConsoleUtils.isWindows()) {
      return null;
    }

    if ("history".equalsIgnoreCase(cmd.trim())) {
      return new HistoryCommand(cmd.trim(), cxt);
    }

    return null;
  }
}

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jline.reader.History;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * Created by zhenhong.gzh on 16/3/17.
 */
public class HistoryCommand extends AbstractCommand {

  private String filter;

  public static final String[] HELP_TAGS = new String[]{"history"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: history [*grep <string>]");
  }

  public void run() throws OdpsException, ODPSConsoleException {
    if (ODPSConsoleUtils.isWindows()) {
      getContext().getOutputWriter().writeError("Not supported on Windows");
      return;
    }
    // red and thick
    String colorStart = "[31;1m";
    String colorEnd = "[0m";

    History history = ODPSConsoleUtils.getOdpsConsoleReader().getHistory();
    Pattern pattern = (filter != null) ? Pattern.compile(filter) : null;
    if (history != null) {
      for (History.Entry entry : history) {
        String line = entry.line();
        if (pattern != null) {
          Matcher matcher = pattern.matcher(line);
          if (!matcher.find()) {
            continue;
          }
          StringBuffer sb = new StringBuffer();
          do {
            matcher.appendReplacement(sb, colorStart + matcher.group() + colorEnd);
          } while (matcher.find());
          matcher.appendTail(sb);
          line = sb.toString();
        }
        getContext().getOutputWriter()
            .writeError(String.valueOf(entry.index()) + ' ' + line);
      }
    }
  }

  public HistoryCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    String[] split = commandText.split("grep ", 2);
    if (split.length > 1) {
      filter = split[1].trim().replaceAll("^\"|\"$", "");
    }
  }

  public static HistoryCommand parse(String cmd, ExecutionContext cxt) {
    if (cmd == null || cxt == null || ODPSConsoleUtils.isWindows()) {
      return null;
    }
    if ("history".equalsIgnoreCase(cmd.trim().split(" ")[0])) {
      return new HistoryCommand(cmd.trim(), cxt);
    }
    return null;
  }
}

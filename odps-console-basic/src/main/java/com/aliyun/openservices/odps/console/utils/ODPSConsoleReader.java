package com.aliyun.openservices.odps.console.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * Created by zhenhong.gzh on 16/1/28.
 */
public class ODPSConsoleReader {

  // 在windows 下和linux下输入方式不一样
  // jline 虽然已经支持 windows，但存在 bug，方向键等特殊按键存在问题
  private Scanner scanner = null;
  private ConsoleReader consoleReader = null;
  private boolean isWindows = false;

  public ODPSConsoleReader() throws ODPSConsoleException {
    init();
  }

  /**
   * 从标准输入读取一行
   *
   * @return 行数据
   */
  public String readLine() {
    return readLine((String) null);
  }

  /**
   * 从标准输入读取一行
   *
   * @param prompt
   *     提示词
   * @return 行数据
   */
  public String readLine(String prompt) {
    return readLine(prompt, null);
  }

  /**
   * 从标准输入读取一行
   *
   * @param prompt
   *     提示词
   * @param mask
   *     输入数据的隐藏符号
   * @return 行数据
   */
  public String readLine(String prompt, final Character mask) {
    //clear the interrupted flag
    Thread.currentThread().interrupted();

    if (isWindows) {
      if (prompt != null) {
        System.out.print(prompt);
      }
      return scanner.nextLine();
    }

    try {
      String input = consoleReader.readLine(prompt, mask);
      return input;
    } catch (UserInterruptException e) {
      if (StringUtils.isNullOrEmpty(e.getPartialLine())) {
        return null;
      } else {
        return "";
      }
    } catch (IOException e) {
      return "";
    }
  }

  public History getHistory() {
    if (consoleReader != null) {
      return consoleReader.getHistory();
    }

    return null;
  }

  public static void setINTSignal() {
    try {
      final Thread currentThread = Thread.currentThread();

      Signal.handle(new Signal("INT"), new SignalHandler() {
        public void handle(Signal sig) {
          if (currentThread.isInterrupted()) {
            System.exit(128 + sig.getNumber());
          } else {
            currentThread.interrupt();
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              //do nothing
            }
            if (currentThread.isInterrupted()) {
              System.err.println("Press Ctrl-C again to exit ODPS console");
            }
          }
        }
      });
    } catch (Exception ignore) {

    }
  }

  private void setSignal() {
    try {
      // reset terminal after Ctrl+Z & fg
      Signal.handle(new Signal("CONT"), new SignalHandler() {
        public void handle(final Signal sig) {
          try {
            consoleReader.getTerminal().reset();
          } catch (Exception e) {
          }
        }
      });
    } catch (Exception ignore) {

    }
    setINTSignal();
  }

  private void setHistory() {
    try {
      final String HISTORYFILE = ".odps_history";
      String historyFile = System.getProperty("user.home") + File.separator + HISTORYFILE;
      consoleReader.setHistory(new FileHistory(new File(historyFile)));
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          try {
            ((FileHistory) consoleReader.getHistory()).flush();
          } catch (IOException ex) {
          }
        }
      });
    } catch (IOException e) {
      // ignore file history failure
    }
  }

  private Completer getCommandCompleter() {
    List<Completer> customCompletor = new ArrayList<Completer>();

    Set<String> candidateStrings = new HashSet<String>();
    try {
      for (String key : CommandParserUtils.getAllCommandKeyWords()) {
        candidateStrings.add(key.toUpperCase());
        candidateStrings.add(key.toLowerCase());
      }
    } catch (AssertionError e) {
      return null;
    }

    if (!candidateStrings.isEmpty()) {
      // odps key word completer, use default whitespace for arg delimiter
      ArgumentCompleter keyCompleter = new ArgumentCompleter(new StringsCompleter(candidateStrings));
      keyCompleter.setStrict(false);
      customCompletor.add(keyCompleter);
    }

    // file patch completer, use default whitespace for arg delimiter
    ArgumentCompleter pathCompleter = new ArgumentCompleter(new FileNameCompleter());
    pathCompleter.setStrict(false);
    customCompletor.add(pathCompleter);

    // aggregate two argument comepletor
    AggregateCompleter aggregateCompleter = new AggregateCompleter(customCompletor);

    return aggregateCompleter;
  }

  private void setCommandCompleter() {
    Completer completer = getCommandCompleter();
    if (completer != null) {
      consoleReader.addCompleter(completer);
    }
  }

  private void init() throws ODPSConsoleException {
    if (ODPSConsoleUtils.isWindows()) {
      isWindows = true;
      scanner = new Scanner(System.in);
    } else {
      try {
        consoleReader = new ConsoleReader();

      } catch (IOException e) {
        throw new ODPSConsoleException(e);
      }

      consoleReader.setExpandEvents(false); // disable jline event
      consoleReader.setHandleUserInterrupt(true);

      setSignal();
      setHistory();
      setCommandCompleter();
    }
  }
}

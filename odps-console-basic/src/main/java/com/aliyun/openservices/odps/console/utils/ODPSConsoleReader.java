package com.aliyun.openservices.odps.console.utils;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.jline.ODPSLineReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import org.jline.builtins.Completers.FileNameCompleter;
import org.jline.reader.Completer;
import org.jline.reader.History;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

/**
 * Created by zhenhong.gzh on 16/1/28.
 */
public class ODPSConsoleReader {

  // 在windows 下和linux下输入方式不一样
  // jline 虽然已经支持 windows，但存在 bug，方向键等特殊按键存在问题
  private Scanner scanner = null;
  private boolean isWindows = false;

  public ODPSConsoleReader() throws ODPSConsoleException {
    if (ODPSConsoleUtils.isWindows()) {
      isWindows = true;
      scanner = new Scanner(System.in);
    }
  }

  public String readConfirmation(String prompt) {
    if (isWindows) {
      return readLine(prompt);
    }

    return ODPSLineReader.getInstance().readLine(prompt, true);
  }

  /**
   * 从标准输入读取一行
   *
   * @return 行数据
   */
  public String readLine() {
    return readLine(null);
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
    Thread.interrupted();

    if (isWindows) {
      if (prompt != null) {
        System.out.print(prompt);
      }
      return scanner.nextLine();
    }

    return ODPSLineReader.getInstance().readLine(prompt, mask);
  }

  public History getHistory() {
      return ODPSLineReader.getInstance().getHistory();
  }
}

package com.aliyun.openservices.odps.console.utils;

import java.util.Scanner;

import org.jline.reader.History;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.jline.ODPSLineReader;

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
      return readLineWindows(prompt);
    }

    return ODPSLineReader.getInstance().readLine(prompt, mask);
  }

  /**
   * Read a line from stdin in Windows
   * @param prompt
   * @return
   */
  private String readLineWindows(String prompt) {
    if (prompt != null) {
      System.err.print(prompt);
    }

    String input = scanner.nextLine();
    StringBuilder inputBuffer = new StringBuilder(input);

    // Read until a line ends with semicolon
    while (!input.trim().endsWith(";")) {
      System.err.print(">");
      input = scanner.nextLine();
      inputBuffer.append(" ").append(input);
    }

    return inputBuffer.toString();
  }

  /**
   * Read a line from stdin, which should not end with semicolon
   * @param prompt
   * @return
   */
  public String readConfirmation(String prompt) {
    if (isWindows) {
      return readConfirmationWindows(prompt);
    }

    return ODPSLineReader.getInstance().readLine(prompt, true);
  }

  /**
   * Read a line from stdin in Windows, which should not end with semicolon
   * @param prompt
   * @return
   */
  public String readConfirmationWindows(String prompt) {
    if (prompt != null) {
      System.err.print(prompt);
    }
    return scanner.nextLine();
  }

  public History getHistory() {
      return ODPSLineReader.getInstance().getHistory();
  }
}

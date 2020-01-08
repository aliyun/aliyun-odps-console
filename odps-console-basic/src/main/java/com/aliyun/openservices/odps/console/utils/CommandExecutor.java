/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.jline.reader.UserInterruptException;

public class CommandExecutor {

  public static class ExecutorResult {

    // CommandExecutor return code
    private final int ecode;
    // get the success string of Shell command
    private final String outStr;
    // get the error string of Shell command
    private final String errorStr;

    public ExecutorResult(int ecode, String outStr, String errorStr) {
      super();
      this.ecode = ecode;
      this.outStr = outStr;
      this.errorStr = errorStr;
    }

    public int getEcode() {
      return ecode;
    }

    public String getOutStr() {
      return outStr;
    }

    public String getErrorStr() {
      return errorStr;
    }

  }

  static class ExecReader extends Thread {

    StringBuffer buffer;
    BufferedReader reader;
    boolean print;

    @Override
    public void run() {
      String line = new String();
      try {
        while ((line = reader.readLine()) != null) {
          buffer.append(line).append("\n");
          if (print) {
            System.err.println(line);
          }

        }
      } catch (IOException e) {
        System.err.print(e.getMessage());
      } catch (NullPointerException e) {
        System.err.print(e.getMessage());
      } catch (Exception e) {
        System.err.print(e.getMessage());
      } finally {
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    public ExecReader(InputStreamReader isr, StringBuffer sb, boolean prt) {
      reader = new BufferedReader(isr);
      buffer = sb;
      print = prt;

    }

  }

  public static ExecutorResult run(String cmd) throws IOException {
    return run(cmd, false);
  }

  public static ExecutorResult run(String cmd, boolean print) throws IOException {
    return run(cmd.split("\\s+"), print);
  }

  public static ExecutorResult run(String cmd, boolean print, File dir) throws IOException {
    return run(cmd.split("\\s+"), print, dir);
  }

  public static ExecutorResult run(String[] cmd, boolean print) throws IOException {
    return run(cmd, print, null);
  }

  public static ExecutorResult run(String[] cmd, boolean print, File dir) throws IOException {
    Runtime runtime = Runtime.getRuntime();
    Process proc = runtime.exec(cmd, null, dir);

    StringBuffer outSB = new StringBuffer();
    StringBuffer errSB = new StringBuffer();

    InputStreamReader osr = new InputStreamReader(proc.getInputStream());
    InputStreamReader esr = new InputStreamReader(proc.getErrorStream());

    ExecReader outReader = new CommandExecutor.ExecReader(osr, outSB, print);
    ExecReader errReader = new CommandExecutor.ExecReader(esr, errSB, print);

    outReader.start();
    errReader.start();
    try {
      proc.waitFor();

      outReader.join();
      errReader.join();
    } catch (InterruptedException e) {
      throw new UserInterruptException(e.getMessage());
    }

    return new ExecutorResult(proc.exitValue(), outSB.toString(), errSB.toString());
  }

}

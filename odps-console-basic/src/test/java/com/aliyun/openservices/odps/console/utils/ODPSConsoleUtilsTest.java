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

import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ODPSConsoleException;

import jline.console.UserInterruptException;

public class ODPSConsoleUtilsTest {

  public void test(String input, String[] expected) {
    String[] r = ODPSConsoleUtils.parseTableSpec(input);
    Assert.assertEquals(expected[0], r[0]);
    Assert.assertEquals(expected[1], r[1]);
  }

  @Test
  public void testParseTableSpec() {
    test("tablename", new String[] { null, "tablename" });
    test("projectname.tablename", new String[] { "projectname", "tablename" });
    test(".tablename", new String[] { null, "tablename" });
    test(".", new String[] { null, null });
    test("", new String[]{null, null});
    test(null, new String[]{null, null});
  }

  @Test
  public void getConfigTest() {
    File file = new File(ODPSConsoleUtils.getConfigFilePath());
    assertTrue(file.exists());
  }

  @Test
  public void getConfigNull() throws Exception {
    URLClassLoader loader = new URLClassLoader(new URL[]{}, this.getClass().getClassLoader()) {
      @Override
      public URL getResource(String name) {
        if (name == "odps_config.ini") {
          return null;
        }
        return super.getResource(name);
      }
    };

    Object returnValue = loader.loadClass("ODPSConsoleUtils").getMethod("getConfigFilePath").invoke(null);
    assertNull(returnValue);
  }

  @Test
  public void testTranslateCommandline() throws ODPSConsoleException {
    String line = "ABC \n \"abc def\" 'd\ne' \t\r\n fge";
    String[] st = ODPSConsoleUtils.translateCommandline(line);
    assertEquals(st[0], "ABC");
    assertEquals(st[1], "\"abc def\"");
    assertEquals(st[2], "'d\ne'");
    assertEquals(st[3], "fge");
    assertEquals(st.length, 4);
  }

  class testTask1 implements Runnable {

    private int count = 0;
    private boolean flag = false;
    public void run() {
      try {
        while (true) {
          ODPSConsoleUtils.checkThreadInterrupted();
          count++;
        }
      } catch (UserInterruptException e) {
        flag = true;
      }

      Assert.assertTrue(flag);
    }
  }

  class testTask2 implements Runnable {

    private boolean flag = false;

    public void run() {
      try {
        while (true) {

          Thread.sleep(10);
        }
      } catch (InterruptedException e) {
        flag = true;
      }

      Assert.assertTrue(flag);
    }
  }

  @Test
  public void testThreadInterrupt() throws InterruptedException {
    Thread t1 = new Thread(new testTask1());
    t1.start();

    Thread.sleep(300);

    t1.interrupt();

    Thread t2 = new Thread(new testTask2());
    t2.start();

    Thread.sleep(300);
    t2.interrupt();
  }
}

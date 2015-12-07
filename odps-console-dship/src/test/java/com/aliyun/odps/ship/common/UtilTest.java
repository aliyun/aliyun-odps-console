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

package com.aliyun.odps.ship.common;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

/**
 * Created by lu.lu on 2014-11-29.
 */
public class UtilTest {

  @Test
  public void testGetSessionBaseDir() {
    String path = Util.getSessionBaseDir();
    System.err.println(path);
    String sessionDir = path + "/custom_session_dir" + System.currentTimeMillis();
    DshipContext.INSTANCE.put(Constants.SESSION_DIR, sessionDir);
    assertEquals("custom_session_dir", sessionDir,
                 Util.getSessionBaseDir());
    assertTrue("custom_session_dir", (new File(sessionDir).exists()));
    DshipContext.INSTANCE.remove(Constants.SESSION_DIR);

    try {
      String sessionFile = path + "/custom_session_file" + System.currentTimeMillis();
      File file = new File(sessionFile);
      file.createNewFile();
      DshipContext.INSTANCE.put(Constants.SESSION_DIR, sessionFile);
      Util.getSessionBaseDir();
      fail("custom session dir can't be file");
    } catch (Exception e) {
      DshipContext.INSTANCE.remove(Constants.SESSION_DIR);
    }
    DshipContext.INSTANCE.remove(Constants.SESSION_DIR);
  }

  @Test
  public void testGetSessionDir() {
    String path = Util.getSessionBaseDir();

    //in ut, root dir is src/test/resources/file
    assertEquals("null", path + "/sessions/null/null", Util.getSessionDir(null));
    assertEquals("abc",
                 path + "/sessions/abc/abc", Util.getSessionDir("abc"));
    assertEquals("abcdefghijk",
                 path + "/sessions/abcdefghijk/abcdefghijk", Util.getSessionDir("abcdefghijk"));
    assertEquals("20141129",
                 path + "/sessions/20141129/20141129", Util.getSessionDir("20141129"));
    assertEquals("20141131",
                 path + "/sessions/20141131/20141131", Util.getSessionDir("20141131"));
    assertEquals("20141129abc",
                 path + "/sessions/20141129/20141129abc", Util.getSessionDir("20141129abc"));
  }

  @Test
  public void testIgnoreCharset() {
    assertTrue(Util.isIgnoreCharset(null));
    assertTrue(Util.isIgnoreCharset(Constants.IGNORE_CHARSET));
    assertFalse(Util.isIgnoreCharset("gbk"));
    assertFalse(Util.isIgnoreCharset("utf-8"));
  }
}

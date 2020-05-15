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

package com.aliyun.odps.ship.history;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

import net.jcip.annotations.NotThreadSafe;

/**
 * 测试SessionHistory的管理
 * */
@NotThreadSafe
public class SessionHistoryManagerTest {
  private static final String TEST_TABLE_NAME = "session_history_manager_test";
  private static String projectName;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    projectName = context.getProjectName();
    DshipContext.INSTANCE.setExecutionContext(context);
    Odps odps = OdpsConnectionFactory.createOdps(context);

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema, true);

    URL url = SessionHistoryManagerTest.class.getResource("/file/sessions/history");
    String sessionPath = new File(url.getPath()).getPath();
    DshipContext.INSTANCE.put(Constants.SESSION_DIR, sessionPath);
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TEST_TABLE_NAME, true);
    DshipContext.INSTANCE.put(Constants.SESSION_DIR, Constants.DEFAULT_SESSION_DIR);
  }

  /**
   * 测试session按session的创建时间排序 . <br/>
   * 测试目的： <br/>
   * 1) 测试session history是否按设置的create time进行排序<br/>
   * **/
  @Test
  public void testListHistory() throws Exception {
    // list session 按session创建的顺序
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream s = new PrintStream(out);

    PrintStream old = System.err;
    System.setErr(s);
    SessionHistoryManager.showHistory(1000);
    String hs = new String(out.toByteArray());

    assertFalse(StringUtils.isNullOrEmpty(hs));

    int i1 = hs.indexOf("s1_session");
    int i2 = hs.indexOf("s2_session");
    int i3 = hs.indexOf("s3_session");

    assertTrue("s1 > s2", i1 > i2);
    assertTrue("s3 > s2", i3 > i2);
    assertTrue("s1 > s3", i1 > i3);
    
    System.setErr(old);
    
  }
  
  /**
   * 测试list session的状态是否符合预期 <br/>
   * **/
  @Test
  public void testListHistoryStatus() throws Exception {

    // list session 按session创建的顺序
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream s = new PrintStream(out);

    PrintStream old = System.err;
    System.setErr(s);
    SessionHistoryManager.showHistory(1000);
    String hs = new String(out.toByteArray());
    int i4 = hs.indexOf("s1_session\tsuccess\t'download");
    int i5 = hs.indexOf("s2_session\tbad\t'upload");
    int i6 = hs.indexOf("s3_session\trunning\t'upload");

    assertFalse(StringUtils.isNullOrEmpty(hs));

    assertTrue("status success", i4 >= 0);
    assertTrue("status bad", i5 >= 0);
    assertTrue("status running", i6 >= 0);

    System.setErr(old);
  }


  /**
   * 测试取默认的session <br/>
   * 测试目的： <br />
   * 1) 测试取最后一个生成的session，即最近的session
   * **/
  @Test
  public void testGetlastestSession() throws Exception {
    // Use default session dir so that other test cases won't be affected
    String testSessionDir = DshipContext.INSTANCE.get(Constants.SESSION_DIR);
    DshipContext.INSTANCE.put(Constants.SESSION_DIR, Constants.DEFAULT_SESSION_DIR);

    String[] args = new String[] {"upload",
            "src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt",
            projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildUploadOption(args);

    //create a session
    String sid = "getlastestsession";
    SessionHistory sh = SessionHistoryManager.createSessionHistory(sid);
    sh.saveContext();

    // 最近一个session
    SessionHistory sh2 = SessionHistoryManager.getLatest();
    assertEquals("getlastestsession", sh2.sid);

    // clear log
    String log = Util.getSessionDir(sid) + "/log.txt";
    File f = new File(log);
    if (f.exists()) {
      f.delete();
    }


    DshipContext.INSTANCE.put(Constants.SESSION_DIR, testSessionDir);
  }

}

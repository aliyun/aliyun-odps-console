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

package com.aliyun.odps.ship.download;

import static org.junit.Assert.assertEquals;

import java.io.FileReader;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.odps.ship.common.OptionsBuilder;


public class FileDownloaderTest {
  @BeforeClass
  public static void setup() throws ODPSConsoleException {
    DshipContext.INSTANCE.setExecutionContext(ExecutionContext.init());
  }

  /**
   * 测试下载数据，包括所有的数据类型。<br/>
   * 测试目的：<br/>
   * 1） mock下载数据的过程，把数据下载到临时文件。<br/>
   * 2） 比较下载数据文件的内容，和原文件一致。<br/>
   * **/
  @Test
  public void testDownloadSlice() throws Exception {

    String[] args =
        new String[]{"download", "up_test_project.test_table/ds='2113',pt='pttest'",
                     "src/test/resources/file/filedownloader/tmp.txt", "-fd=||", "-rd=\n",
                     "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildDownloadOption(args);

    MockDownloadSession us = new MockDownloadSession();
    FileDownloader
        sd = new FileDownloader("src/test/resources/file/filedownloader/tmp.txt", 0L, 0L, 10L, us, null);
    sd.download();
    assertEquals("download", readFile("src/test/resources/file/filedownloader/sample.txt"),
                 readFile("src/test/resources/file/filedownloader/tmp.txt"));
  }

  /**
   * 测试下载，当下载出错时，下载行为符合预期。<br/>
   * 测试目的：<br/>
   * 1） 模拟出错4次, downloader retry成功。<br/>
   * 2） 下载文件内容正确<br/>
   * **/
  @Test
  public void testDownloadFileWithCrash4_Success() throws Exception {

    String[] args =
        new String[] {"download", "up_test_project.test_table/ds='2113',pt='pttest'",
            "src/test/resources/file/filedownloader/tmp.txt", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildDownloadOption(args);
    MockDownloadSession us = new MockDownloadSession();
    MockDownloadSession.crash = 4;
    FileDownloader
        sd = new FileDownloader("src/test/resources/file/filedownloader/tmp.txt", 0L, 0L, 10L, us, null);
    sd.download();

    assertEquals("download", readFile("src/test/resources/file/filedownloader/sample.txt"),
        readFile("src/test/resources/file/filedownloader/tmp.txt"));
  }

  /**
   * 测试读数据出错5次，下载失败<br/>
   * 测试目的：<br/>
   * 1） 读数据时，连接出错5次，下载失败，异常信息符合预期.<br/>
   * **/
  @Test
  public void testDownloadFileWithCrash5_Fail() throws Exception {

    String[] args =
        new String[] {"download", "up_test_project.test_table/ds='2113',pt='pttest'",
            "src/test/resources/file/filedownloader/tmp.txt", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildDownloadOption(args);
    MockDownloadSession us = new MockDownloadSession();
    MockDownloadSession.crash = 5;

    FileDownloader
        sd = new FileDownloader("src/test/resources/file/filedownloader/tmp.txt", 0L, 0L, 10L, us, null);
    try {
      sd.download();
    } catch (Exception e) {
      assertEquals("error message", "ERROR: download read error, retry exceed 5.\ncrash :5", e.getMessage());
    }
  }

  private String readFile(String filename) throws Exception {

    StringBuilder sb = new StringBuilder();
    FileReader r = new FileReader(filename);
    int c = r.read();
    while (c != -1) {
      sb.append((char) c);
      c = r.read();
    }
    r.close();
    return sb.toString();
  }

}

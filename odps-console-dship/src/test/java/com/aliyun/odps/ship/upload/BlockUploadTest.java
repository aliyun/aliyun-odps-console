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

package com.aliyun.odps.ship.upload;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * 测试Block上传
 * */
public class BlockUploadTest {

  @BeforeClass
  public static void setup() throws ODPSConsoleException {
    DshipContext.INSTANCE.setExecutionContext(ExecutionContext.init());
  }

  /**
   * 测试上传一个Block成功，上传成功，检查finish block 状态符合预期
   * */
  @Test
  public void testUploadSingleFile() throws Exception {

    String[] args =
        new String[] {"upload",
            "src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    // test upload src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt
    OptionsBuilder.buildUploadOption(args);

    //set unique upload id
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_upload_single_file" + System.currentTimeMillis());
    MockUploadSession us = new MockUploadSession();
    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.saveContext();
    sh.loadContext();
    String blockInfo = "1:0:156:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);
    blockUploader.upload();
    List<BlockInfo> blockList = sh.loadFinishBlockList();
    assertEquals("finish block is not 1", blockList.size(), 1);
    assertEquals("block id is not 1", blockList.get(0).getBlockId(), Long.valueOf(1L));
  }

  /**
   * 测试--discard-bad-records=false，不丢弃脏数据，当有脏数据时出错
   * */
  @Test
  public void testFailDiscardBadRecordsFalse() throws Exception {

    String[] args =
        new String[] {"upload", "src/test/resources/file/fileuploader/badrecords/badrecords3.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildUploadOption(args);
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_fail_discard_bad_records_false" + System.currentTimeMillis());
    MockUploadSession us = new MockUploadSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileuploader/badrecords/badrecords3.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);

    try {
      blockUploader.upload();
      fail("don't throw exception on bad record");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
          e.getMessage().indexOf("ERROR: format error") == 0);
    }
  }

  /**
   * 测试默认情况下 --strict-schema=true，严格检查数据的 schema, 当有数据字段少于 schema 出错
   * */
  @Test
  public void testBadSchemaFail() throws Exception {

    String[] args =
        new String[] {"upload", "src/test/resources/file/fileuploader/badrecords/badschema.txt",
                      "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
                      "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildUploadOption(args);
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_fail_strict_schema_true" + System.currentTimeMillis());
    MockUploadSession us = new MockUploadSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileuploader/badrecords/badschema.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);

    try {
      blockUploader.upload();
      fail("don't throw exception on bad schema");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("ERROR: column mismatch, expected 6 columns, 5 columns found, please check data or delimiter") == 0);
    }
  }

  /**
   * 测试 --strict-schema=false，不严格检查数据的 schema, 当有数据字段少时，填充 NULL； 数据多了直接抛弃
   * */
  @Test
  public void testBadSchemaSuccess() throws Exception {

    String[] args =
        new String[] {"upload", "src/test/resources/file/fileuploader/badrecords/badschema.txt",
                      "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
                      "-dfp=yyyyMMddHHmmss", "-ss=false"};
    OptionsBuilder.buildUploadOption(args);
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_fail_strict_schema_false" + System.currentTimeMillis());
    MockUploadSession us = new MockUploadSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileuploader/badrecords/badschema.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);

    blockUploader.upload();
    List<BlockInfo> blockList = sh.loadFinishBlockList();
    assertEquals("finish block is not 1", blockList.size(), 1);
    assertEquals("block id is not 1", blockList.get(0).getBlockId(), Long.valueOf(1L));
  }

  /**
   * 测试最大discard bad records的边界，并检查脏数据信息 如果大于max-bad-records，则出错，如果小于，则导入数据,把数据存到bad.txt文件中
   * */
  @Test
  public void testSuccessDiscardBadRecordsMaxSize() throws Exception {
    String[] args =
        new String[] {"upload", "src/test/resources/file/fileuploader/badrecords/badrecords3.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss", "-dbr", "true"};
    OptionsBuilder.buildUploadOption(args);
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_success_discard_bad_records_true" + System.currentTimeMillis());
    DshipContext.INSTANCE.put(Constants.MAX_BAD_RECORDS, "3");
    MockUploadSession us = new MockUploadSession();
    us.clearSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileuploader/badrecords/badrecords3.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);

    blockUploader.upload();
    List<BlockInfo> blockList = sh.loadFinishBlockList();
    assertEquals("finish block is not 1", blockList.size(), 1);
    assertEquals("block id is not 1", blockList.get(0).getBlockId(), Long.valueOf(1L));

    // 3 record bad data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream s = new PrintStream(out);
    PrintStream old = System.out;
    System.setOut(s);
    sh.showBad();
    String hs = new String(out.toByteArray(), "UTF-8");
    String[] sp = hs.split("\n");
    /**
     * bad data a||ab测试c||20130308101010||true||21.2||123456.1234
     * 234||bb你好b||20130508101010||b||2.2||1234.1234 333||ccc||20130308100910||false||2.2||c
     **/
    assertTrue("first line", sp[0].indexOf("a||ab") == 0);
    assertTrue("second line", sp[1].indexOf("234||bb") == 0);
    assertTrue("last line", sp[2].indexOf("333||ccc") == 0);
    assertTrue("expect 3, but :" + sp.length, sp.length == 3);
    System.setOut(old);
  }

  @Test
  public void testFailDiscardBadRecordsMaxSize() throws Exception {
    String[] args =
        new String[] {"upload", "src/test/resources/file/fileuploader/badrecords/badrecords3.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss", "-dbr", "true"};
    OptionsBuilder.buildUploadOption(args);
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID,
                              "test_Fail_discard_bad_records_true" + System.currentTimeMillis());
    DshipContext.INSTANCE.put(Constants.MAX_BAD_RECORDS, "2");
    MockUploadSession us = new MockUploadSession();
    us.clearSession();

    SessionHistory sh = SessionHistoryManager.createSessionHistory(us.getUploadId());
    sh.loadContext();
    String blockInfo = "1:0:300:src/test/resources/file/fileuploader/badrecords/badrecords3.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);

    try {
        blockUploader.upload();
        fail("bad record don't reach max limit");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("ERROR: bad records exceed 2") == 0);
    }

    // 2 record bad data
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream s = new PrintStream(out);
    PrintStream old = System.out;
    System.setOut(s);
    sh.showBad();
    String hs = new String(out.toByteArray());
    String[] sp = hs.split("\n");
    /**
     * bad data a||ab测试c||20130308101010||true||21.2||123456.1234
     * 234||bb你好b||20130508101010||b||2.2||1234.1234
     **/
    assertTrue("first line", sp[0].indexOf("a||ab") == 0);
    assertTrue("second line", sp[1].indexOf("234||bb") == 0);
    assertTrue("bad line 2", sp.length == 2);
    System.setOut(old);
  }

  /**
   * 测试上传文件时，同一个block上传出错5次，正常上传
   * */
  @Test
  public void testUploadFailRetry5() throws Exception {

    String[] args =
        new String[] {"upload",
            "src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    // test upload src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt
    OptionsBuilder.buildUploadOption(args);

    //set unique upload id
    String uploadId = "test_upload_fail_retry5" + System.currentTimeMillis();
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID, uploadId);
    MockErrorUploadSession us = new MockErrorUploadSession();
    us.setCrash(5);

    SessionHistory sh = SessionHistoryManager.createSessionHistory(uploadId);
    sh.loadContext();
    String blockInfo = "2:0:156:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);
    blockUploader.upload();
    List<BlockInfo> blockList = sh.loadFinishBlockList();
    assertEquals("finish block is not 1", blockList.size(), 1);
    assertEquals("block id is not 2", blockList.get(0).getBlockId(), Long.valueOf(2L));
  }

  /**
   * 测试上传文件时，同一个block上传6次，上传失败
   * */
  @Test
  public void testUploadFailRetry6() throws Exception {

    String[] args =
        new String[] {"upload",
            "src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    // test upload src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt
    OptionsBuilder.buildUploadOption(args);

    //set unique upload id
    String uploadId = "test_upload_fail_retry6" + System.currentTimeMillis();
    DshipContext.INSTANCE.put(Constants.RESUME_UPLOAD_ID, uploadId);
    MockErrorUploadSession us = new MockErrorUploadSession();
    us.setCrash(6);

    SessionHistory sh = SessionHistoryManager.createSessionHistory(uploadId);
    sh.loadContext();
    String blockInfo = "2:0:156:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt";
    BlockInfo block = new BlockInfo();
    block.parse(blockInfo);
    BlockUploader blockUploader = new BlockUploader(block, us, sh);
    try {
      blockUploader.upload();
      fail("need fail");
    } catch (Exception e) {
      assertEquals("error no equal", "write error", e.getMessage());
    }
  }

}

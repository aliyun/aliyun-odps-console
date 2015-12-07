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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * 测试BlockInfo
 * */
public class BlockInfoTest {

  /**
   * 测试创建单个文件的BlockIndex
   * */
  @Test
  public void testParseBlockInfo() throws Exception {

    BlockInfo blockInfo = new BlockInfo();

    //positive case
    blockInfo.parse("1:0:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    assertEquals("block info 1", blockInfo.toString(), "1:0:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");

    //negative case
    blockInfo.parse("1:0:50:D:/src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    assertEquals("block info 1", blockInfo.toString(), "1:0:50:D:/src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");

    //negative case 
    // blockId can't parse to Long
    try {
      blockInfo.parse("not long:0:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
      fail("need fail, blockId must be long");
    } catch (Exception e) {
      //do nothing
    }

    //negative case 
    // startPos can't parse to Long
    try {
      blockInfo.parse("1:not long:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
      fail("need fail, startPos must be long");
    } catch (Exception e) {
      //do nothing
    }

    //negative case 
    // length can't parse to Long
    try {
      blockInfo.parse("1:0:not long:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
      fail("need fail, length must be long");
    } catch (Exception e) {
      //do nothing
    }

    //negative case 
    // blockId can't parse to Long
    try {
      blockInfo.parse("not long:0:50:");
      fail("need fail, filePath can't empty");
    } catch (Exception e) {
      //do nothing
    }

    //negative case 
    //must has at least 4 segment
    try {
      blockInfo.parse("1:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
      fail("need fail, must has at least 4 segment");
    } catch (Exception e) {
      //do nothing
    }

  }

}


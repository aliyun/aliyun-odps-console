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

import com.aliyun.odps.ship.common.BlockInfo;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * 测试创建BlockIndex
 * */
public class BlockInfoBuilderTest {

  /**
   * 测试创建单个文件的BlockIndex
   * */
  @Test
  public void testSingleFile() throws Exception {

    File file = new File("src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    BlockInfoBuilder blockIndexBuilder = new BlockInfoBuilder();

    //split a file into single block
    ArrayList<BlockInfo> buildIndex = blockIndexBuilder.buildBlockIndex(file);
    long totalBytes = blockIndexBuilder.getFileSize(file);
    assertEquals("file size", 156L, totalBytes);
    assertEquals("block size", buildIndex.size(), 1);
    assertEquals("block info", buildIndex.get(0).toString(), "1:0:156:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");

    //split a file into multi block
    blockIndexBuilder.setBlockSize(50);
    buildIndex = blockIndexBuilder.buildBlockIndex(file);
    assertEquals("block size", buildIndex.size(), 4);
    assertEquals("block info 1", buildIndex.get(0).toString(), "1:0:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    assertEquals("block info 2", buildIndex.get(1).toString(), "2:50:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    assertEquals("block info 3", buildIndex.get(2).toString(), "3:100:50:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");
    assertEquals("block info 4", buildIndex.get(3).toString(), "4:150:6:src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt");

  }

  /**
   * 测试创建目录的BlockIndex
   * 只创建一级目录的Index，忽略子目录
   * */
  @Test
  public void testFileFolder() throws Exception {

    File file = new File("src/test/resources/file/fileuploader/foldertest");
    BlockInfoBuilder blockIndexBuilder = new BlockInfoBuilder();

    //split per file into per block
    ArrayList<BlockInfo> buildIndex = blockIndexBuilder.buildBlockIndex(file);
    long totalBytes = blockIndexBuilder.getFileSize(file);
    assertEquals("file size", 468L, totalBytes);
    assertEquals("block size", buildIndex.size(), 3);
    assertEquals("block info 1", buildIndex.get(0).toString(), "1:0:156:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese1.txt");
    assertEquals("block info 2", buildIndex.get(1).toString(), "2:0:156:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese2.txt");
    assertEquals("block info 3", buildIndex.get(2).toString(), "3:0:156:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese3.txt");

    //split a file into multi block
    blockIndexBuilder.setBlockSize(100);
    buildIndex = blockIndexBuilder.buildBlockIndex(file);
    assertEquals("block size", buildIndex.size(), 6);
    assertEquals("block info 1", buildIndex.get(0).toString(), "1:0:100:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese1.txt");
    assertEquals("block info 2", buildIndex.get(1).toString(), "2:100:56:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese1.txt");
    assertEquals("block info 3", buildIndex.get(2).toString(), "3:0:100:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese2.txt");
    assertEquals("block info 4", buildIndex.get(3).toString(), "4:100:56:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese2.txt");
    assertEquals("block info 5", buildIndex.get(4).toString(), "5:0:100:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese3.txt");
    assertEquals("block info 6", buildIndex.get(5).toString(), "6:100:56:src/test/resources/file/fileuploader/foldertest/mock_upload_more_char_split_chinese3.txt");

  }

  /**
   * 测试创建空目录的BlockIndex
   * */
  @Test
  public void testEmptyFolder() throws Exception {

    File nullfolder = new File("src/test/resources/file/fileuploader/nullfolder");
    File file = new File("src/test/resources/file/fileuploader/nullfolder");
    BlockInfoBuilder blockIndexBuilder = new BlockInfoBuilder();

    //split per file into per block
    ArrayList<BlockInfo> buildIndex = blockIndexBuilder.buildBlockIndex(file);
    long totalBytes = blockIndexBuilder.getFileSize(file);
    assertEquals("file size", 0L, totalBytes);
    assertEquals("block size", buildIndex.size(), 0);
}

}

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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * 测试SessionHistory
 * */
public class SessionHistoryTest {
  @BeforeClass
  public static void setup() throws ODPSConsoleException {
    DshipContext.INSTANCE.setExecutionContext(ExecutionContext.init());
  }

  /**
   * 测试loadBlockIndex和loadFinishBlockList <br/>
   * **/
  @Test
  public void testSaveAndLoadBlockIndex() throws Exception {

    String sid = "test_block_list";

    SessionHistory sh = new SessionHistory(sid);
    sh.delete();

    sh = new SessionHistory(sid);
    
    String[] args =
        new String[] {"upload",
            "src/test/resources/file/fileuploader/mock_upload_more_char_split_chinese.txt",
            "up_test_project.test_table/ds='2113',pt='pttest'", "-fd=||", "-rd=\n",
            "-dfp=yyyyMMddHHmmss"};
    OptionsBuilder.buildUploadOption(args);
    sh.saveContext();

    ArrayList<BlockInfo> blockIndex =  new ArrayList<BlockInfo>() {{
                                    add(new BlockInfo(1L, new File("block1"), 0L, 1L));
                                    add(new BlockInfo(2L, new File("block2"), 0L, 1L));
                                    add(new BlockInfo(3L, new File("block3"), 0L, 1L));
                                    add(new BlockInfo(4L, new File("block4"), 0L, 1L));
                                    add(new BlockInfo(5L, new File("block5"), 0L, 1L));
                                    }};
    sh.saveBlockIndex(blockIndex);
    blockIndex = sh.loadBlockIndex();
    assertEquals("total block size", blockIndex.size(), 5);

    sh.saveFinishBlock(new BlockInfo(1L, new File("block1"), 0L, 1L));
    sh.saveFinishBlock(new BlockInfo(2L, new File("block2"), 0L, 1L));
    blockIndex = sh.loadBlockIndex();
    assertEquals("unfinish block size", blockIndex.size(), 3);
    assertEquals("block info", blockIndex.get(0).getBlockId(), Long.valueOf(3L));
    assertEquals("block info", blockIndex.get(1).getBlockId(), Long.valueOf(4L));
    assertEquals("block info", blockIndex.get(2).getBlockId(), Long.valueOf(5L));

    List<BlockInfo> finishList = sh.loadFinishBlockList();
    assertEquals("finish block size", finishList.size(), 2);
    assertEquals("finish block info", finishList.get(0).getBlockId(), Long.valueOf(1L));
    assertEquals("finish block info", finishList.get(1).getBlockId(), Long.valueOf(2L));
  }
  
}

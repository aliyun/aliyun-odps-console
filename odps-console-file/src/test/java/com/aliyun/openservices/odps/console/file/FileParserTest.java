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

package com.aliyun.openservices.odps.console.file;

import static org.junit.Assert.*;

import org.junit.Test;

public class FileParserTest {

    @Test
    public void testParse() throws Exception{

        FileOperationCommand fileCommand = FileOperationCommand.parse(
                "fs -ls    /", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-ls", fileCommand.paras[1]);
        assertEquals("/", fileCommand.paras[2]);
        
        
        fileCommand = FileOperationCommand.parse(
                "fs -ls    /v1/p1/f1", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-ls", fileCommand.paras[1]);
        assertEquals("/v1/p1/f1", fileCommand.paras[2]);
        
        
        fileCommand = FileOperationCommand.parse(
                "fs -mkv   /gan", null);
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-mkv", fileCommand.paras[1]);
        assertEquals("/gan", fileCommand.paras[2]);
        
        
        fileCommand = FileOperationCommand.parse(
                "fs -rmv   /gan", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-rmv", fileCommand.paras[1]);
        assertEquals("/gan", fileCommand.paras[2]);
        
        fileCommand = FileOperationCommand.parse(
                "fs -rmp   /gan/pp", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-rmp", fileCommand.paras[1]);
        assertEquals("/gan/pp", fileCommand.paras[2]);

        
        fileCommand = FileOperationCommand.parse(
                "fs -mkv test_volume_comment 'commenttest'", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-mkv", fileCommand.paras[1]);
        assertEquals("test_volume_comment", fileCommand.paras[2]);
        assertEquals("'commenttest'", fileCommand.paras[3]);

        fileCommand = FileOperationCommand.parse(
                "fs -put  data/testfile   /test_volume/filepartition/testfile", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-put", fileCommand.paras[1]);
        assertEquals("data/testfile", fileCommand.paras[2]);
        assertEquals("/test_volume/filepartition/testfile", fileCommand.paras[3]);
        
        fileCommand = FileOperationCommand.parse(
                "fs -put  data/testfolder/   /test_volume/filepartition/test", null);
        
        assertNotNull(fileCommand);
        assertEquals("fs", fileCommand.paras[0]);
        assertEquals("-put", fileCommand.paras[1]);
        assertEquals("data/testfolder/", fileCommand.paras[2]);
        assertEquals("/test_volume/filepartition/test", fileCommand.paras[3]);
        
    }
    
  @Test
  public void testParseFsMeta()throws Exception {

    FileOperationCommand fileCommand = FileOperationCommand.parse("fs -meta    /vtest/", null);

    assertNotNull(fileCommand);
    assertEquals("fs", fileCommand.paras[0]);
    assertEquals("-meta", fileCommand.paras[1]);
    assertEquals("/vtest/", fileCommand.paras[2]);
    
  }

}

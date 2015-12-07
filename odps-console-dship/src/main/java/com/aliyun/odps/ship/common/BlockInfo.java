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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by lulu on 15-2-04.
 */
public class BlockInfo {
  private Long blockId;
  private File file;
  private Long startPos;
  private Long length;

  public BlockInfo() {
  }

  public BlockInfo(Long blockId, File file, Long startPos, Long length) {
    this.blockId = blockId;
    this.file= file;
    this.startPos = startPos;
    this.length = length;
  }


  public void parse(String blockInfo) {
    String[] splits = blockInfo.split(":");
    if (splits.length < 4) {
      throw new IllegalArgumentException("BlockInfo's format should be <blockId>:<startPos>:<length>:<filePath>. " +
          "now is " + blockInfo);
    }
    blockId = Long.valueOf(splits[0]);
    startPos = Long.valueOf(splits[1]);
    length = Long.valueOf(splits[2]);
    file= new File(getFilePath(blockInfo));
  }

  public Long getBlockId() {
    return blockId;
  }

  public File getFile() {
    return file;
  }
  
  public InputStream getFileInputStream() throws IOException {
    return new FileInputStream(file);
  }

  public Long getStartPos() {
    return startPos;
  }

  public Long getLength() {
    return length;
  }

  @Override
  public String toString() {
    return "" + blockId + ":" + startPos + ":" + length + ":" + file.getPath();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof BlockInfo)) {
      return false;
    }

    if (obj == this) {
      return true;
    }

    BlockInfo blockInfo = (BlockInfo) obj;
    return (this.blockId == blockInfo.getBlockId()) && this.file.getPath().equals(blockInfo.getFile().getPath())
        && (this.startPos == blockInfo.getStartPos()) && (this.length == blockInfo.getLength());
  }

  private String getFilePath(String blockInfo) {
    String sep = ":";
    int fromIndex = 0;
    int occurs = 0;
    int index = 0;
    while ((index = blockInfo.indexOf(sep, fromIndex)) != -1)
    {
      if (index == blockInfo.length() - 1) {
        break;
      }

      occurs ++;
      if (occurs == 3) {
        return blockInfo.substring(index + 1);
      }
      fromIndex = index + 1;
    }

    throw new IllegalArgumentException("BlockInfo's format should be <blockId>:<startPos>:<length>:<filePath>. " +
       "now is " + blockInfo);
    
  }
}

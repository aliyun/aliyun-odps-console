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
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.tunnel.TunnelException;

import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by lulu on 15-2-04.
 */
public class BlockInfoBuilder {
  private long blockSize = Constants.DEFAULT_BLOCK_SIZE * 1024 * 1024;

  public BlockInfoBuilder() {
  }

  public ArrayList<BlockInfo> buildBlockIndex(File file)
            throws IOException, TunnelException, ParseException {
    ArrayList<BlockInfo> blockIndex = new ArrayList<BlockInfo>();
    build(file, blockIndex);
    return blockIndex;
  }

  public void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
  }

  public long getFileSize(File file)
            throws IOException, TunnelException, ParseException {
    long totalBytes = 0;
    if (file.isDirectory()) {
      File[] fileList = file.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.isFile();
        }
      });
      fileList = Util.sortFiles(fileList);

      for (File f : fileList) {
        totalBytes += getFileSize(f);
      }
    } else {
      long fileLength = file.length();
      totalBytes += fileLength;
    }
    return totalBytes;
  }

  private void build(File file, ArrayList<BlockInfo> blockIndex)
            throws IOException, TunnelException, ParseException {
    if (file.isDirectory()) {
      File[] fileList = file.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.isFile();
        }
      });
      fileList = Util.sortFiles(fileList);

      for (File f : fileList) {
        build(f, blockIndex);
      }
    } else {
      long fileLength = file.length();
      long i = 0;

      while (i < fileLength) {
        long length = i + blockSize < fileLength ? blockSize : fileLength - i;
        BlockInfo blockInfo = new BlockInfo(Long.valueOf(blockIndex.size() + 1), file, i, length);
        blockIndex.add(blockInfo);
        i += length;
      }
    }
  }
}

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import jline.console.UserInterruptException;

/**
 * Created by lulu on 15-1-29.
 */
public class DshipUpload {

  private TunnelUploadSession tunnelUploadSession;
  private String sessionId;
  private SessionHistory sessionHistory;

  private boolean resume;
  private File uploadFile;
  private long totalUploadBytes;
  private ArrayList<BlockInfo> blockIndex = new ArrayList<BlockInfo>();
  private long blockSize = Constants.DEFAULT_BLOCK_SIZE * 1024 * 1024;
  private long startTime = System.currentTimeMillis();

  public DshipUpload()
      throws TunnelException, IOException, ParseException, ODPSConsoleException {
    resume = (DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID) != null);
    tunnelUploadSession = new TunnelUploadSession();
    sessionId = tunnelUploadSession.getUploadId();
    sessionHistory = SessionHistoryManager.createSessionHistory(sessionId);
    sessionHistory.saveContext();

    this.uploadFile = new File(DshipContext.INSTANCE.get(Constants.RESUME_PATH));
    this.totalUploadBytes = 0;
    if (DshipContext.INSTANCE.get(Constants.BLOCK_SIZE) != null) {
      blockSize = Long.valueOf(DshipContext.INSTANCE.get(Constants.BLOCK_SIZE)) * 1024 * 1024;
    }
    buildIndex(uploadFile);
  }


  public void upload() throws TunnelException, IOException, ParseException {

    System.err.println("Start upload:" + uploadFile.getPath());
    if (!resume) {
      System.err.println(
          "Total bytes:" + totalUploadBytes + "\t Split input to " + blockIndex.size() + " blocks");
    } else {
      System.err.println("Resume " + blockIndex.size() + " blocks");
    }
    sessionHistory.log("start upload:" + uploadFile.getPath());

    String scan = DshipContext.INSTANCE.get(Constants.SCAN);
    if (isScan(scan)) {
      tunnelUploadSession.setScan(true);
      //only scan, don't really upload
      uploadBlock();
    }

    if (isUpload(scan)) {
      tunnelUploadSession.setScan(false);
      //really upload
      uploadBlock();
      List<BlockInfo> finishBlockList = sessionHistory.loadFinishBlockList();
      List<Long> finishBlockIdList = new ArrayList<Long>();
      for (BlockInfo block : finishBlockList) {
        finishBlockIdList.add(block.getBlockId());
      }
      tunnelUploadSession.complete(finishBlockIdList);
      long gap = (System.currentTimeMillis() - startTime) / 1000;
      if (gap > 0) {
        long avgSpeed = totalUploadBytes / gap / 1024;
        System.err.println("upload complete, average speed is " + avgSpeed + " KB/s");
      }
    }

    sessionHistory.log("upload complete:" + uploadFile.getPath());

    DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.success.toString());
    sessionHistory.saveContext();

    System.err.println("OK");
  }

  public String getTunnelSessionId() {
    return tunnelUploadSession.getUploadId();
  }

  private void uploadBlock()
      throws IOException, TunnelException, ParseException {
    int threads = Integer.valueOf(DshipContext.INSTANCE.get(Constants.THREADS));
    ExecutorService executors = Executors.newFixedThreadPool(threads);
    ArrayList<Callable<Long>> callList = new ArrayList<Callable<Long>>();
    for (BlockInfo block : blockIndex) {
      final BlockUploader
          uploader =
          new BlockUploader(block, tunnelUploadSession, sessionHistory);
      Callable<Long> call = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          uploader.upload();
          return 0L;
        }
      };

      callList.add(call);
    }

    try {
      List<Future<Long>> futures = executors.invokeAll(callList);
      ArrayList<String> failedBlock = new ArrayList<String>();
      for (int i = 0; i < futures.size(); ++i) {
        try {
          futures.get(i).get();
        } catch (ExecutionException e) {
          e.printStackTrace();
          failedBlock.add(String.valueOf(i));
        }
      }
      if (!failedBlock.isEmpty()) {
        throw new TunnelException("Block ID:" + StringUtils.join(failedBlock, ",") + " Failed.");
      }
    } catch (InterruptedException e) {
      throw new UserInterruptException(e.getMessage());
    }
  }

  private void buildIndex(File file)
      throws IOException, TunnelException, ParseException {
    if (!resume) {
      BlockInfoBuilder blockIndexBuilder = new BlockInfoBuilder();
      blockIndexBuilder.setBlockSize(blockSize);

      blockIndex = blockIndexBuilder.buildBlockIndex(file);
      totalUploadBytes = blockIndexBuilder.getFileSize(file);
      sessionHistory.saveBlockIndex(blockIndex);
    } else {
      blockIndex = sessionHistory.loadBlockIndex();
    }
  }

  private boolean isScan(String scan) throws ParseException {
    if (scan.equalsIgnoreCase("true") || scan.equalsIgnoreCase("only")) {
      return true;
    } else if (scan.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new ParseException("Unrecognized command, '-scan='" + scan);
    }
  }

  private boolean isUpload(String scan) throws ParseException {
    if (scan.equalsIgnoreCase("true") || scan.equalsIgnoreCase("false")) {
      return true;
    } else if (scan.equalsIgnoreCase("only")) {
      return false;
    } else {
      throw new ParseException("Unrecognized command, '-scan='" + scan);
    }
  }

}

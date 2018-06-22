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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;
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

  // this array is sorted by its item length
  private final String[] recordDelimiterArray = {"\r\n", "\n"};
  private final int checkRDBlockSize = Constants.MAX_RECORD_SIZE / 20;

  public DshipUpload()
      throws OdpsException, IOException, ParseException, ODPSConsoleException {
    resume = (DshipContext.INSTANCE.get(Constants.RESUME_UPLOAD_ID) != null);

    tunnelUploadSession = new TunnelUploadSession();
    sessionId = tunnelUploadSession.getUploadId();
    sessionHistory = SessionHistoryManager.createSessionHistory(sessionId);

    this.uploadFile = new File(DshipContext.INSTANCE.get(Constants.RESUME_PATH));
    this.totalUploadBytes = 0;
    if (DshipContext.INSTANCE.get(Constants.BLOCK_SIZE) != null) {
      blockSize = Long.valueOf(DshipContext.INSTANCE.get(Constants.BLOCK_SIZE)) * 1024 * 1024;
    }
    checkRecordDelimiter();
    sessionHistory.saveContext();

    buildIndex(uploadFile);
  }


  private void checkRecordDelimiter() throws TunnelException, IOException {
    // to find out the RECORD_DELIMITER if it is null
    if (DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER) == null) {
      String
          fileRecordDelimiter =
          getRecordDelimiter(uploadFile, checkRDBlockSize, recordDelimiterArray);
      DshipContext.INSTANCE.put(Constants.RECORD_DELIMITER,
                                fileRecordDelimiter == null ? Constants.DEFAULT_RECORD_DELIMITER
                                                            : fileRecordDelimiter);
    }

  }

  private String getRecordDelimiter(File file, int checkSize, String[] delimiterArray)
      throws TunnelException, IOException {
    String recordDelimiter = null;

    byte[] buf = new byte[checkSize];

    if (file.isDirectory()) {
      boolean hasFile = false;

      File[] fileList = file.listFiles();
      for (File fileName : fileList) {
        if (fileName.isFile() && (0 < FileUtils.sizeOf(fileName))) {
          file = fileName;
          hasFile = true;
          break;
        }
      }

      if (!hasFile) {
        return recordDelimiter;
      }
    }

    BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
    try {
      int len = is.read(buf, 0, checkSize);
      if (len != -1) {
        for (String delimiter : delimiterArray) {
          int res = BlockRecordReader.indexOf(buf, 0, len, delimiter.getBytes());
          if (res != -1) {
            recordDelimiter = delimiter;
            break;
          }
        }
      }
    } finally {
      IOUtils.closeQuietly(is);
    }

    return recordDelimiter;
  }

  public void upload() throws TunnelException, IOException, ParseException {
    System.err.println("Start upload:" + uploadFile.getPath());
    System.err.println("Using " + StringEscapeUtils.escapeJava(
        DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER)) + " to split records");
    System.err.println("Upload in strict schema mode: " + DshipContext.INSTANCE.get(Constants.STRICT_SCHEMA));

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
        long avgSpeed = totalUploadBytes / gap;
        System.err
            .println("upload complete, average speed is " + Util.toReadableBytes(avgSpeed) + "/s");
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
    } finally {
      executors.shutdownNow();
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

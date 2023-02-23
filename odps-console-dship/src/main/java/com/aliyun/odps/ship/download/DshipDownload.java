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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.PartitionHelper;
import com.aliyun.odps.ship.common.SessionStatus;
import com.aliyun.odps.ship.common.Util;
import com.aliyun.odps.ship.history.SessionHistory;
import com.aliyun.odps.ship.history.SessionHistoryManager;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import com.google.common.io.Files;

import org.jline.reader.UserInterruptException;

/**
 * Created by nizheming on 15/5/27.
 */
public class DshipDownload {

  private ArrayList<FileDownloader> workItems = new ArrayList<FileDownloader>();
  private int threads;
  private String path;
  private long writtenBytes = 0L;
  private Long limit;
  private ExecutionContext context;
  private String projectName;
  private String schemaName;
  private String tableName;
  private String instanceId;
  private String partitonSpecLiteral;
  private String ext;
  private String filename;
  private String parentDir;
  private long totalLines;
  private long slices;
  private boolean isCsv;

  SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public DshipDownload() {
    threads = Integer.parseInt(DshipContext.INSTANCE.get(Constants.THREADS));
    if (DshipContext.INSTANCE.get(Constants.LIMIT) != null) {
      limit = Long.parseLong(DshipContext.INSTANCE.get(Constants.LIMIT));
    } else {
      limit = null;
    }
    path = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    projectName = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    schemaName = DshipContext.INSTANCE.get(Constants.SCHEMA);
    tableName = DshipContext.INSTANCE.get(Constants.TABLE);
    instanceId = DshipContext.INSTANCE.get(Constants.INSTANE_ID);
    partitonSpecLiteral = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    ext = Files.getFileExtension(path);
    filename = Files.getNameWithoutExtension(path);
    parentDir = FilenameUtils.removeExtension(path) + File.separator;
    context = DshipContext.INSTANCE.getExecutionContext();
    isCsv = "true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.CSV_FORMAT));
  }

  public void initInstanceDownloadWorkItems(Odps odps)
      throws IOException, ParseException, ODPSConsoleException, OdpsException {
    splitDataByThreads(new TunnelDownloadSession(instanceId), null);
  }

  public void initTableDownloadWorkItems(Odps odps)
      throws IOException, ParseException, ODPSConsoleException, OdpsException {

    PartitionHelper helper = new PartitionHelper(odps, projectName, schemaName, tableName);

    if (!helper.isPartitioned()) {
      if (partitonSpecLiteral != null) {
        throw new OdpsException(
            Constants.ERROR_INDICATOR + "can not specify partition for an unpartitioned table");
      }
      splitDataByThreads(new TunnelDownloadSession(tableName, null), null);
    } else {
      List<PartitionSpec> parSpecs = helper.inferPartitionSpecs(partitonSpecLiteral);
      if (parSpecs.size() == 0) {
        throw new OdpsException(Constants.ERROR_INDICATOR + "can not infer any partitions from: "
                                + partitonSpecLiteral);
      } else if (parSpecs.size() == 1) {
        // 对于指定分区数为 1 的表，退化为下载整个表的情况，分片数量等于使用线程的数量
        PartitionSpec ps = parSpecs.get(0);
        splitDataByThreads(new TunnelDownloadSession(tableName, ps), ps);
      } else {
        // 对于指定分区数大于 2 的表，分片数量等于下载分区的数量
        slices = parSpecs.size();
        long sliceId = 0;
        long start = 0;
        for (PartitionSpec ps : parSpecs) {
          if (limit != null && start == limit) {
            break;
          }

          TunnelDownloadSession tds = new TunnelDownloadSession(tableName, ps);
          SessionHistory sh = SessionHistoryManager.createSessionHistory(tds.getDownloadId());
          String
              msg =
              ps.toString() + "\tnew session: " + tds.getDownloadId() + "\ttotal lines: " + Util
                  .toReadableNumber(tds.getTotalLines());
          System.err.println(sim.format(new Date()) + "  -  " + msg);
          sh.log(msg);

          long
              step =
              (limit == null) ? tds.getTotalLines() : Math.min(tds.getTotalLines(), limit - start);

          String sliceFileName = filename + PartitionHelper.buildSuffix(ps);
          if (StringUtils.isNotEmpty(ext)) {
            sliceFileName = sliceFileName + "." + ext;
          }
          path = parentDir + sliceFileName;
          FileDownloader sd = new FileDownloader(path, sliceId, 0L, step, tds, sh, isCsv, ps);
          workItems.add(sd);
          sliceId++;
          start += step;
        }
        totalLines = start;
      }
    }
  }

  public void download() throws IOException, ParseException, ODPSConsoleException, OdpsException {
    Odps odps = OdpsConnectionFactory.createOdps(context);
    //TODO schema rm this
    if (projectName == null) {
      projectName = odps.getDefaultProject();
    }

    if (instanceId != null) {
        // download instance
      initInstanceDownloadWorkItems(odps);
    } else {
      initTableDownloadWorkItems(odps);
      // download table
    }

    long startTime = System.currentTimeMillis();
    for (final FileDownloader sd : workItems) {
      DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.running.toString());
      sd.sh.saveContext();
    }

    if (threads == 1) {
      System.err.printf("downloading %s records into %s\n", Util.toReadableNumber(totalLines),
                        Util.pluralize("file", slices));
      for (final FileDownloader sd : workItems) {
        sd.download();
        writtenBytes += sd.getWrittenBytes();
      }
    } else {
      System.err.printf("downloading %s records into %s using %s\n",
                        Util.toReadableNumber(totalLines), Util.pluralize("file", slices),
                        Util.pluralize("thread", threads));
      multiThreadDownload();
    }

    for (final FileDownloader sd : workItems) {
      DshipContext.INSTANCE.put(Constants.STATUS, SessionStatus.success.toString());
      sd.sh.saveContext();
    }
    long gap = System.currentTimeMillis() - startTime;
    if (gap > 0) {
      long avgSpeed = (writtenBytes / gap) * 1000;
      System.err.printf("total: %s, time: %s, average speed: %s/s\n",
                        Util.toReadableBytes(writtenBytes), Util.toReadableMilliseconds(gap),
                        Util.toReadableBytes(avgSpeed));
    }
    System.err.println("download OK");
  }

  private void splitDataByThreads(TunnelDownloadSession tds, PartitionSpec ps)
      throws FileNotFoundException, ODPSConsoleException, IOException, TunnelException {
    SessionHistory sh = SessionHistoryManager.createSessionHistory(tds.getDownloadId());
    String
        msg =
        "new session: " + tds.getDownloadId() + "\ttotal lines: " + Util
            .toReadableNumber(tds.getTotalLines());
    System.err.println(sim.format(new Date()) + "  -  " + msg);
    sh.log(msg);

    // 分片数量等于使用线程的数量
    slices = threads;
    long start = 0;
    totalLines = (limit == null) ? tds.getTotalLines() : Math.min(limit, tds.getTotalLines());
    long step = (totalLines + slices - 1) / slices;
    for (long i = 0; i < slices; i++) {
      long end = Math.min(start + step, totalLines);
      if (slices != 1) {  //多个分片时，添加分片后缀
        String sliceFileName = filename + "_" + i;
        if (StringUtils.isNotEmpty(ext)) {
          sliceFileName = sliceFileName + "." + ext;
        }
        path = parentDir + sliceFileName;
      }
      FileDownloader sd = new FileDownloader(path, i, start, end, tds, sh, isCsv, ps);
      workItems.add(sd);
      start = end;
    }
  }

  private void multiThreadDownload() throws TunnelException {
    ArrayList<Callable<Long>> callList = new ArrayList<Callable<Long>>();
    for (final FileDownloader downloader : workItems) {
      Callable<Long> call = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          downloader.download();
          return downloader.getWrittenBytes();
        }
      };
      callList.add(call);
    }

    ExecutorService executors = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Long>> futures = executors.invokeAll(callList);
      ArrayList<String> failedThread = new ArrayList<String>();
      for (int i = 0; i < futures.size(); ++i) {
        try {
          writtenBytes += futures.get(i).get();
        } catch (ExecutionException e) {
          e.printStackTrace();
          failedThread.add(String.valueOf(i));
        }
      }
      if (!failedThread.isEmpty()) {
        throw new TunnelException("Slice ID:" + StringUtils.join(failedThread, ",") + " Failed.");
      }
    } catch (InterruptedException e) {
      throw new UserInterruptException(e.getMessage());
    } finally {
      executors.shutdownNow();
    }
  }
}

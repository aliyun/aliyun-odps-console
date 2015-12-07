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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Project;
import com.aliyun.odps.Volume;
import com.aliyun.odps.VolumeFile;
import com.aliyun.odps.VolumePartition;
import com.aliyun.odps.tunnel.VolumeTunnel;
import com.aliyun.odps.tunnel.VolumeTunnel.DownloadSession;
import com.aliyun.odps.tunnel.VolumeTunnel.UploadSession;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class UploadDownloadUtil {

  // 上传文件
  public static void uploadFile(File file, UploadSession up, String blockName) throws ODPSConsoleException {

    try {
      if (blockName.startsWith("/")) {
        blockName = blockName.substring(1);
      }

      OutputStream output = up.openOutputStream(blockName);
      FileInputStream input = new FileInputStream(file);

      copy(input, output);

      System.out.println(file.getAbsolutePath());
    } catch (Exception e) {
      throw new ODPSConsoleException(e.getMessage(), e.getCause());
    }
  }

  // 上传目录
  public static void uploadFolder(File file, UploadSession up, String prefix) throws ODPSConsoleException {

    File[] files = file.listFiles();
    for (File f : files) {

      if (f.isFile()) {
        uploadFile(f, up, prefix + "/" + f.getName());
      }
    }
  }

  // 下载目录
  public static void downloadFolder(String source, String desFolder, String partition, String projectName,
      Volume volume, VolumeTunnel tunnel) throws ODPSConsoleException {
    desFolder = desFolder + (desFolder.endsWith("/") ? "" : "/");

    List<String> blocks = getBlocks(source, partition, volume);

    for (String bs : blocks) {

      String blockName = bs;
      if (blockName.startsWith("/")) {
        blockName = blockName.substring(1);
      }
      try {
        DownloadSession down = tunnel.createDownloadSession(projectName, volume.getName(), partition, blockName);

        File file = new File(desFolder + bs.substring(source.length()));
        downloadFile(file, down);

      } catch (Exception e) {
        throw new ODPSConsoleException(e.getMessage(), e.getCause());
      }

    }
  }

  // 下载文件
  public static void downloadFile(File file, DownloadSession down) throws ODPSConsoleException {

    try {

      if (file.getParentFile() != null) {
        file.getParentFile().mkdirs();
      }

      FileOutputStream output = new FileOutputStream(file);
      InputStream input = down.openInputStream(0, Long.MAX_VALUE);

      copy(input, output);

      System.out.println(file.getAbsolutePath());
    } catch (Exception e) {
      throw new ODPSConsoleException(e.getMessage(), e.getCause());
    }
  }

  /**
   * path 是block的相对路径
   * 
   * */
  private static List<String> getBlocks(String source, String partition, Volume volume) throws ODPSConsoleException {

    List<String> blocks = new ArrayList<String>();

    try {
      VolumePartition p = volume.getVolumePartition(partition);
      Iterator<VolumeFile> fs = p.getFileIterator(source);
      while (fs.hasNext()) {
        VolumeFile f = fs.next();
        String name = f.getName();
        if (name.endsWith("/")) {
          // 目录的一定是"/"结束
          blocks.addAll(getBlocks(source + name, partition, volume));
        } else {
          blocks.add(source + name);
        }
      }
    } catch (Exception e) {
      throw new ODPSConsoleException(e.getMessage(), e.getCause());
    }

    return blocks;

  }

  private static void copy(InputStream input, OutputStream output) throws Exception {

    byte[] buffer = new byte[10240];
    int bytesRead;
    while ((bytesRead = input.read(buffer)) != -1) {
      output.write(buffer, 0, bytesRead);
    }

    output.flush();
    output.close();

    input.close();
  }
}

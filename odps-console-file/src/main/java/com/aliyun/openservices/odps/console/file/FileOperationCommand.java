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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.Volume;
import com.aliyun.odps.VolumeFile;
import com.aliyun.odps.VolumePartition;
import com.aliyun.odps.tunnel.VolumeTunnel;
import com.aliyun.odps.tunnel.VolumeTunnel.DownloadSession;
import com.aliyun.odps.tunnel.VolumeTunnel.UploadSession;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class FileOperationCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"file", "fs", "volume"};

  public static void printUsage(PrintStream out) {
    out.println("");
    out.println("Usage: fs <-ls [-l] | -rmv | -rmp > <path>;     list or remove volume[partition]");
    out.println("       fs <-put | -get > <src> <dest>;          put  or get file[dir].");
    out.println("       fs <-mkv > <volume_name> [comment];      create volume.");
    out.println("       fs <-meta > <path>;                      desc meta.");
    out.println("For example:");
    out.println("       fs -ls /;   list volumes.");
    out.println("       fs -put /home/admin/data /vname/pname;   ");
    out.println("       fs -mkv test_volume  'for test';    ");
  }

  String[] paras;

  public FileOperationCommand(String commandText, ExecutionContext context, String[] paras) {
    super(commandText, context);

    this.paras = paras;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    String path = paras[2];
    Project project = odps.projects().get();

    if (paras[1].equalsIgnoreCase("-ls") && paras.length == 3) {

      listFile(odps, path, false);

    } else if (paras[1].equalsIgnoreCase("-ls") && paras[2].equalsIgnoreCase("-l")
        && paras.length == 4) {
      
      path = paras[3];
      listFile(odps, path, true);
      
    } else if ((paras[1].equalsIgnoreCase("-rmv") || paras[1].equalsIgnoreCase("-rmp"))
        && paras.length == 3) {

      rmFile(odps, path, paras[1]);

    } else if (paras[1].equalsIgnoreCase("-put") && paras.length == 4) {

      putFile();

    } else if (paras[1].equalsIgnoreCase("-get") && paras.length == 4) {

      getFile(odps);

    } else if (paras[1].equalsIgnoreCase("-mkv")) {

      String comment = "";
      String volumeName = paras[2];
      if (paras.length > 3) {
        String command = getCommandText();
        comment = command.substring(command.toUpperCase().indexOf(" " + volumeName + " ") + volumeName.length() + 2);
      }

      odps.volumes().create(project.getName(), volumeName, comment);

    } else if (paras[1].equalsIgnoreCase("-meta") && paras.length == 3) {

      descMeta(odps);

    } else {
      throwUsageError(System.err, "Invalid parameters - Generic options must be specified.");
    }

    getWriter().writeError("OK");
  }

  private static void throwUsageError(PrintStream out, String msg) throws ODPSConsoleException {
    printUsage(out);
    throw new ODPSConsoleException(msg);
  }

  public static FileOperationCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (commandString.toUpperCase().matches("FS\\s+.*")) {

      String temp[] = commandString.trim().split("\\s+");

      if (temp.length >= 3) {
        return new FileOperationCommand(commandString, sessionContext, temp);
      } else {
        throwUsageError(System.err, "Invalid parameters - Generic options must be specified.");
      }
    }

    return null;
  }

  private void getFile(Odps odps) throws ODPSConsoleException {

    int start = paras[2].startsWith("/") ? 1 : 0;
    String[] sp = paras[2].substring(start).split("/");
    String vName = sp[0];
    String pName = "";
    String filePath = "/";

    if (sp.length > 1) {
      pName = sp[1];
    } else {
      throwUsageError(System.err, "Invalid parameters - pls set partition name.");
    }

    if (sp.length > 2) {
      filePath = paras[2].substring(start + sp[0].length() + sp[1].length() + 2);
    }

    String tunnelEndpoint = getContext().getTunnelEndpoint();

    VolumeTunnel tunnel = new VolumeTunnel(odps);
    if (!StringUtils.isEmpty(tunnelEndpoint)) {
      tunnel.setEndpoint(tunnelEndpoint);
    }

    try {

      if (filePath.endsWith("/")) {

        Volume volume = odps.volumes().get(vName);
        String projectName = odps.projects().get().getName();
        UploadDownloadUtil.downloadFolder(filePath, paras[3], pName, projectName, volume, tunnel);

      } else if (!filePath.equals("")) {

        if (filePath.startsWith("/")) {
          filePath = filePath.substring(1);
        }

        DownloadSession down = tunnel.createDownloadSession(getCurrentProject(), vName, pName, filePath);
        File file = new File(paras[3]);
        UploadDownloadUtil.downloadFile(file, down);

      }

    } catch (Exception e) {
      throw new ODPSConsoleException(e.getMessage(), e.getCause());
    }
  }

  private void descMeta(Odps odps) throws ODPSConsoleException {

    int start = paras[2].startsWith("/") ? 1 : 0;
    String[] sp = paras[2].substring(start).split("/");
    String vName = sp[0].equals("") ? "/" : sp[0];
    String pName = null;
    String path = "/";

    if (sp.length > 1) {
      pName = sp[1];
    }

    if (sp.length > 2) {
      path = paras[2].substring(start + sp[0].length() + sp[1].length() + 2);
    }

    if (pName == null) {
      Volume v = odps.volumes().get(vName);
      System.out.println("Comment: " + v.getComment());
      System.out.println("Length: " + v.getLength());
      System.out.println("File number: " + v.getFileCount());

    } else {
      Volume v = odps.volumes().get(vName);
      VolumePartition m = v.getVolumePartition(pName);
      System.out.println("Length: " + m.getLength());
      System.out.println("File number: " + m.getFileCount());
    }

  }

  private void putFile() throws ODPSConsoleException {
    int start = paras[3].startsWith("/") ? 1 : 0;

    String[] sp = paras[3].substring(start).split("/");
    String vName = sp[0];
    String pName = "";
    String filePath = "/";

    if (sp.length > 1) {
      pName = sp[1];
    } else {
      throwUsageError(System.err, "Invalid parameters - pls set partition name.");
    }

    if (sp.length > 2) {
      filePath = paras[3].substring(start + sp[0].length() + sp[1].length() + 2);
    }

    try {

      File file = new File(paras[2]);
      if (!file.exists()) {
        throw new ODPSConsoleException("file not exists.");
      }

      Odps odps = getCurrentOdps();

      String tunnelEndpoint = getContext().getTunnelEndpoint();
      
      VolumeTunnel tunnel = new VolumeTunnel(odps);
      if (!StringUtils.isEmpty(tunnelEndpoint)) {
        tunnel.setEndpoint(tunnelEndpoint);
      }
      UploadSession up = tunnel.createUploadSession(getCurrentProject(), vName, pName);

      if (file.exists() && file.isDirectory()) {

        filePath = (filePath.endsWith("/") ? filePath : filePath + "/") + file.getName();

        UploadDownloadUtil.uploadFolder(file, up, filePath);
      } else if (file.exists() && file.isFile()) {
        UploadDownloadUtil.uploadFile(file, up, filePath.equals("/") ? file.getName() : filePath);
      }

      up.commit(up.getFileList());
    } catch (Exception e) {
      throw new ODPSConsoleException(e.getMessage(), e.getCause());
    }
  }

  private void rmFile(Odps odps, String path, String option) throws ODPSConsoleException, OdpsException {

    int start = path.startsWith("/") ? 1 : 0;
    String[] sp = path.substring(start).split("/");
    String vName = sp[0];
    String pName = "";

    if (sp.length > 1) {
      pName = sp[1];
    }
    if (sp.length > 2) {
      throwUsageError(System.err, "Invalid parameters - can not remove file or folder.");
    }

    if (!"".equals(pName) && option.equalsIgnoreCase("-rmp")) {
      Volume v = odps.volumes().get(vName);
      v.deleteVolumePartition(pName);

    } else if (!"".equals(vName) && "".equals(pName) && option.equalsIgnoreCase("-rmv")) {
      Project project = odps.projects().get();
      odps.volumes().delete(project.getName(), vName);

    } else {
      throwUsageError(System.err, "Invalid parameters - unrecognized option [" + path + "].");
    }
  }

  private void listFile(Odps odps, String path, boolean isShowAll) throws OdpsException {
    int start = path.startsWith("/") ? 1 : 0;

    String[] sp = path.substring(start).split("/");

    String vName = sp[0];
    String pName = "";
    String filePath = "/";

    if (sp.length > 1) {
      pName = sp[1];
    }

    if (sp.length > 2) {
      filePath = path.substring(start + sp[0].length() + sp[1].length() + 2);
    }

    List<String[]> volumeList = new ArrayList<String[]>();
    String[] volumeTitle = null;
    int[] columnPercent = null;

    if (isShowAll) {
      volumeTitle = new String[] { "Owner", "LastModifyTime", "FileCount", "Length", "FileName", };
      columnPercent = new int[] { 20, 15, 6, 8, 49 };
      volumeList.add(volumeTitle);
    }

    int totalCount = 0;

    if (!StringUtils.isBlank(pName)) {
      Volume v = odps.volumes().get(vName);
      VolumePartition p = v.getVolumePartition(pName);
      Iterator<VolumeFile> fs = p.getFileIterator(filePath);
      filePath = filePath.equals("/") ? "/" : "/" + filePath + (filePath.endsWith("/") ? "" : "/");
      int fs_size = 0;
      while (fs.hasNext()) {
        fs_size++;
        VolumeFile f = fs.next();
        getWriter().writeResult("/" + vName + "/" + pName + filePath + f.getName());
      }
      getWriter().writeError("total " + (fs_size));
      return;
    } else if (!StringUtils.isBlank(vName)) {
      Volume v = odps.volumes().get(vName);
      Iterator<VolumePartition> ps = v.getPartitionIterator();
      int ps_size = 0;
      while (ps.hasNext()) {
        ps_size++;
        VolumePartition p = ps.next();

        if (isShowAll) {
          String[] vAttr = new String[5];
          vAttr[0] = p.getOwner();
          vAttr[1] = p.getLastModifiedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
              .getLastModifiedTime());
          vAttr[2] = p.getFileCount() + "";
          vAttr[3] = p.getLength() + "";
          vAttr[4] = "/" + vName + "/" + p.getName() + "/";
          volumeList.add(vAttr);
        } else {
          getWriter().writeResult("/" + vName + "/" + p.getName() + "/");
        }
      }
      totalCount = ps_size;
    } else {
      Iterator<Volume> vs = odps.volumes().iterator();
      int vs_size = 0;
      while (vs.hasNext()) {
        vs_size++;
        Volume v = vs.next();
        if (isShowAll) {
          String[] vAttr = new String[5];
          vAttr[0] = v.getOwner();
          vAttr[1] = v.getLastModifiedTime() == null ? " " : ODPSConsoleUtils.formatDate(v
              .getLastModifiedTime());
          vAttr[2] = v.getFileCount() + "";
          vAttr[3] = v.getLength() + "";
          vAttr[4] = "/" + v.getName() + "/";
          volumeList.add(vAttr);
        } else {
          getWriter().writeResult("/" + v.getName() + "/");
        }
      }
      totalCount = vs_size;
    }
    if (isShowAll) {
      ODPSConsoleUtils.formaterTable(volumeList, columnPercent, getContext().getConsoleWidth());
    }
    getWriter().writeError("total " + (totalCount));
  }
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.Util;

public class SessionHistory {

  String sid;
  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  SessionHistory(String sid) {
    this.sid = sid;

    File f = new File(Util.getSessionDir(sid));
    if (!f.exists()) {
      f.mkdirs();
    }

  }

  public void saveContext() throws FileNotFoundException, IOException {

    String cp = Util.getSessionDir(sid) + "/context.properties";

    Properties p = new Properties();
    p.putAll(DshipContext.INSTANCE.getAll());
    FileOutputStream out = new FileOutputStream(cp);
    try {
      p.store(out, "context");
    } finally {
      IOUtils.closeQuietly(out);
    }
  }

  public void loadContext() throws FileNotFoundException, IOException {
    //DshipContext.INSTANCE.clear();
    Map<String, String> ctx = loadProperty();
    DshipContext.INSTANCE.putAll(ctx);
  }

  public Map<String, String> loadProperty() throws FileNotFoundException, IOException {

    String cp = Util.getSessionDir(sid) + "/context.properties";
    File file = new File(cp);
    if (!file.exists()) {
      //null context
      return new HashMap<String, String>();
    }

    Map<String, String> context = new HashMap<String, String>();
    FileInputStream in = new FileInputStream(file);
    try {
      Properties p = new Properties();
      p.load(in);
      Set<Entry<Object, Object>> entrySet = p.entrySet();
      for (Entry<Object, Object> entry : entrySet) {
        if (!entry.getKey().toString().startsWith("#")) {
          context.put(((String) entry.getKey()), ((String) entry.getValue()));
        }
      }
    } finally {
      IOUtils.closeQuietly(in);
    }
    return context;
  }


  public void log(String msg) throws FileNotFoundException, IOException {

    String log = Util.getSessionDir(sid) + "/log.txt";
    String m = df.format(new Date()) + "  -  " + msg + "\n";
    write(new File(log), m, true);
  }

  public void clearBadData(long bid) {

    String bad = Util.getSessionDir(sid) + "/bad_" + bid;
    File f = new File(bad);
    if (f.exists()) {
      f.delete();
    }
  }

  public void delete() {
    String s = Util.getSessionDir(sid);
    File f = new File(s);
    if (f.exists()) {
      File[] fs = f.listFiles();
      for (File df : fs) {
        df.delete();
      }
      f.delete();
    }
    File parentFile = f.getParentFile();
    if (parentFile.list().length == 0) {
      parentFile.delete();
    }
  }

  public void saveBadData(String data, long bid) throws IOException {

    String bad = Util.getSessionDir(sid) + "/bad_" + bid;
    write(new File(bad), data, true);
  }

  public void showLog() throws FileNotFoundException, IOException {

    String log = Util.getSessionDir(sid) + "/log.txt";
    show(new File(log));
  }

  protected boolean existsBad() {

    File dir = new File(Util.getSessionDir(sid));
    String[] bl = dir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.indexOf("bad_") == 0;
      }
    });
    return bl != null && bl.length > 0;
  }

  public void showBad() throws FileNotFoundException, IOException {

    String bad = Util.getSessionDir(sid);

    File[] fs = new File(bad).listFiles();
    fs = Util.sortFiles(fs);
    for (File f : fs) {
      if (f.getName().indexOf("bad_") == 0) {
        show(f);
      }
    }
  }

  public void saveBlockIndex(ArrayList<BlockInfo> blockIndex) throws FileNotFoundException, IOException {
    File blockIndexFile = new File(Util.getSessionDir(sid) + "/block_index.txt");

    if (blockIndexFile.exists()) {
      blockIndexFile.delete();
    }

    for (BlockInfo blockInfo : blockIndex) {
      write(blockIndexFile, blockInfo.toString() + "\n", true);
    }
  }

  public void saveFinishBlock(BlockInfo blockInfo) throws FileNotFoundException, IOException {
    File finishBlockFile = new File(Util.getSessionDir(sid) + "/finish_block.txt");
    write(finishBlockFile, blockInfo.toString() + "\n", true);
  }

  public ArrayList<BlockInfo> loadBlockIndex() throws IOException {

    ArrayList<BlockInfo> blockIndex = new ArrayList<BlockInfo>();
    //step 1 load all block from block_index.txt
    File blockIndexFile = new File(Util.getSessionDir(sid) + "/block_index.txt");
    if (!blockIndexFile.exists()) {
      return blockIndex;
    }
    BufferedReader blockIndexReader = new BufferedReader(new FileReader(blockIndexFile));
    try {
      String blockInfo = null;
      while ((blockInfo = blockIndexReader.readLine()) != null) {
        BlockInfo block = new BlockInfo();
        block.parse(blockInfo);
        blockIndex.add(block);
      }
    } finally {
      IOUtils.closeQuietly(blockIndexReader);
    }
    //step2 remove block exists in finish_block_index.txt
    File finishBlockFile = new File(Util.getSessionDir(sid) + "/finish_block.txt");
    if (!finishBlockFile.exists()) {
      return blockIndex;
    }
    BufferedReader finishBlockReader = new BufferedReader(new FileReader(finishBlockFile));
    try {
      String finishBlock = null;
      while ((finishBlock = finishBlockReader.readLine()) != null) {
        BlockInfo block = new BlockInfo();
        block.parse(finishBlock);
        blockIndex.remove(block);
      }
    } finally {
      IOUtils.closeQuietly(finishBlockReader);
    }
    return blockIndex;
  }

  public List<BlockInfo> loadFinishBlockList() throws IOException{
    List<BlockInfo> blockList = new ArrayList<BlockInfo>();
    File finishBlockFile = new File(Util.getSessionDir(sid) + "/finish_block.txt");
    if (!finishBlockFile.exists()) {
      return blockList;
    }

    BufferedReader finishBlockReader = new BufferedReader(new FileReader(finishBlockFile));

    try {
      String finishBlock = null;
      while ((finishBlock = finishBlockReader.readLine()) != null) {
        BlockInfo block = new BlockInfo();
        block.parse(finishBlock);
        blockList.add(block);
      }
    } finally {
      IOUtils.closeQuietly(finishBlockReader);
    }

    return blockList;
  }


  private void show(File f) throws IOException {
    FileInputStream fileInputStream = new FileInputStream(f);

    try {
      InputStreamReader r = new InputStreamReader(fileInputStream, "utf-8");
      int c = r.read();
      while (c != -1) {
        System.out.print((char) c);
        c = r.read();
      }
      r.close();
    } catch (UnsupportedEncodingException e) {

    } finally {
      IOUtils.closeQuietly(fileInputStream);
    }
  }

  private void write(File f, String data, boolean append) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(f, append);
    try {
      OutputStreamWriter w = new OutputStreamWriter(fileOutputStream, "utf-8");
      w.write(data);
      w.close();
    } catch (UnsupportedEncodingException e) {

    } finally {
      IOUtils.closeQuietly(fileOutputStream);
    }
  }

  public String getSid() {
    return sid;
  }

}

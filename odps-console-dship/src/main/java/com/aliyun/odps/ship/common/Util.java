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
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class Util {

  private static String getRootDir() {

    String path = Util.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (path.endsWith(".jar")) {
      try {
        return URLDecoder.decode(path.substring(0, path.lastIndexOf("/")) + "/..", "UTF-8");
      } catch (UnsupportedEncodingException e) {
      }
    }

    return new File(ODPSConsoleUtils.getConfigFilePath()).getParent() + "/file";
  }

  public static String getSessionBaseDir() throws IllegalArgumentException{

    String basePath = getRootDir();

    if (DshipContext.INSTANCE.get(Constants.SESSION_DIR) != null) {
      try {
        basePath = URLDecoder.decode(DshipContext.INSTANCE.get(Constants.SESSION_DIR), "UTF-8");
      } catch  (UnsupportedEncodingException e) {
      }
    }

    File baseDir = new File(basePath);
    if (!baseDir.exists()) {
      baseDir.mkdirs();
    } else if (baseDir.isFile()) {
      throw new IllegalArgumentException("SessionDir must be directory, now is a file.");
    }
    return basePath;
  }

  //if sid is from tunnel, its format like 2014112910082427d0610a001da849, the first 8 chars is the date sid created.
  //we cut the first 8 char as subdir, so session's dir is <root_dir>/sessions/<date>/<session_id>
  // in other case, create session dir like <root_dir>/sessions/<session_id>/<session_id>. PS, this case is for ut
  public static String getSessionDir(String sid) throws IllegalArgumentException{
    String subdir; 
    if (sid == null) {
      subdir = null;
    } else if (sid.length() > 8) {
      try {
        DateFormat format = new SimpleDateFormat("yyyyMMdd");
        format.parse(sid.substring(0, 8));
        subdir = sid.substring(0, 8);
      } catch (ParseException e) {
        subdir = sid;
      }
    } else {
      subdir = sid;
    }
    return getSessionBaseDir() + "/sessions/" + subdir + "/" + sid;
  }

  public static String getStack(Exception e) {

    StringWriter errors = new StringWriter();
    PrintWriter w = new PrintWriter(errors);
    e.printStackTrace(w);
    w.close();
    return errors.toString();
  }

  public static File[] sortFiles(File[] fs){
    
    if (fs == null){
      return null;
    }
    //sort files
    List<File> files = Arrays.asList(fs);
    Collections.sort(files, new Comparator<File>() {
      @Override
      public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    
    return files.toArray(new File[files.size()]);
  }
  
  public static void checkSession(String sid)throws FileNotFoundException{
    
    if (sid == null){
      return;
    }
    File f = new File(getSessionDir(sid));
    if (!f.exists()) {
      throw new FileNotFoundException(Constants.ERROR_INDICATOR + "session '" + sid
          + "' not found");
    }
  }
  
  public static boolean isWindows(){
    
    String osName = System.getProperties().getProperty("os.name");
    return osName.toLowerCase().indexOf("windows")>=0;
  }

  public static String toHumanReadableString(String in) {
    String ret = StringEscapeUtils.escapeJava(in);
    if (ret == null) {
      return "<null>";
    } else if (ret.equals("")) {
      return "\"\"(empty string)";
    }
    return "\"" + ret + "\"";
  }

  public static boolean isIgnoreCharset(String charset) {
    return charset == null || charset.toLowerCase().equals(Constants.IGNORE_CHARSET);
  }

  public static String toReadableBytes(long bytes) {
    DecimalFormat df = new DecimalFormat("###,###.#");
    if (bytes < 1024) {
      return df.format(bytes) + " bytes";
    } else if (bytes < 1024 * 1024) {
      return df.format((float) bytes / 1024) + " KB";
    } else if (bytes < 1024 * 1024 * 1024) {
      return df.format((float) bytes / 1024 / 1024) + " MB";
    } else {
      return df.format((float) bytes / 1024 / 1024 / 1024) + " GB";
    }
  }

  public static String toReadableSeconds(long sec) {
    if (sec < 60) {
      return sec + " s";
    } else {
      return sec / 60 + " m "+ sec % 60 + " s";
    }
  }

  public static String toReadableNumber(long number) {
    DecimalFormat df = new DecimalFormat("###,###");
    return df.format(number);
  }

  public static String pluralize(String word, long count) {
    if (count == 1) {
      return count + " " + word;
    } else {
      return count + " " + word + "s";
    }
  }
}

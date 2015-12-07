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

package com.aliyun.openservices.odps.console.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

public class FileUtil {

  public static void saveInputStreamToFile(InputStream inputStream, String filePath)
      throws Exception {

    final File dwFile = new File(filePath);
    // inputstream to file
    OutputStream outputStream = null;
    try {
      outputStream = new BufferedOutputStream(new FileOutputStream(dwFile));
      byte[] buffer = new byte[1024 * 10];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) > -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      throw new OdpsException("无法读取内容。", e);
    } finally {
      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException e) {
        }
        try {
          inputStream.close();
        } catch (IOException e) {
        }
      }
    }
  }

  public static String getStringFromFile(String fileName) throws ODPSConsoleException {

    FileInputStream fis = null;
    try {
      File file = new File(fileName);
      fis = new FileInputStream(file);
      return IOUtils.readStreamAsString(fis);
    } catch (FileNotFoundException e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.FILE_NOT_EXIST);
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage());
    } finally {
      try {
        if (fis != null) {
          fis.close();
        }
      } catch (IOException e) {
      }
    }
  }

}

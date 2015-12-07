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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.CountingOutputStream;


public class TextRecordWriter {

  private final int BUFFER_SIZE = 8 * 1024 * 1024;
  CountingOutputStream os;
  byte[] fd;
  byte[] rd;

  public TextRecordWriter(File file, String fd, String rd) throws FileNotFoundException {

    this.os = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(file), BUFFER_SIZE));
    this.fd = fd.getBytes();
    this.rd = rd.getBytes();
  }

  public void write(byte[][] line) throws IOException {

    for (int i = 0; i < line.length; i++) {
      os.write(line[i]);
      if (i < line.length - 1) {
        os.write(fd);
      }
    }
    os.write(rd);
  }

  public void close() throws IOException {
    os.close();
  }

  public long getWrittedBytes() {
    return os.getByteCount();
  }

}

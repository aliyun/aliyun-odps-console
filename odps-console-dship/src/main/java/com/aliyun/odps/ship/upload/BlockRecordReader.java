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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

import com.aliyun.odps.ship.common.BlockInfo;
import com.aliyun.odps.ship.common.Constants;

public class BlockRecordReader {

  private BlockInfo blockInfo;
  private byte[] fieldDelimiter;
  private byte[] recordDelimiter;
  boolean ignoreHeader;


  private BufferedInputStream is;
  private long startPos;
  private long readBytes;
  private byte[] currentLine;
  boolean isLastLine;

  String detectedCharset;
  int bomBytes;

  private ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(8 * 1024 * 1024);
  private final int BUF_SIZE = 8 * 1024 * 1024;
  private byte[] buf = new byte[BUF_SIZE];
  private int bufLength = 0;
  private int offset = 0;

  public BlockRecordReader(BlockInfo blockInfo, String fd, String rd, boolean ignoreHeader)
      throws IOException {
    this.blockInfo = blockInfo;
    this.fieldDelimiter = fd.getBytes();
    this.recordDelimiter = rd.getBytes();
    this.ignoreHeader = ignoreHeader;

    init();
  }

  public byte[][] readTextRecord() throws IOException, ParseException {
    if (isLastLine) {
      return null;
    }
    currentLine = readLine();
    isLastLine = startPos + readBytes > blockInfo.getStartPos() + blockInfo.getLength();
    return splitLine(currentLine);
  }

  public String getCurrentLine() {
    return new String(currentLine);
  }

  protected byte[][] splitLine(byte[] sbl) {

    if (sbl == null) {
      return null;
    }
    List<byte[]> l = new ArrayList<byte[]>();
    int start = 0;
    int end = 0;
    while (end != -1) {
      end = indexOf(sbl, start, sbl.length, fieldDelimiter);
      int colLen = (end == -1 ? sbl.length - start : end - start);
      byte[] col = new byte[colLen];
      System.arraycopy(sbl, start, col, 0, colLen);
      l.add(col);
      start = end + fieldDelimiter.length;
    }
    return l.toArray(new byte[l.size()][]);
  }

  private int count = 0;

  protected byte[] readLine() throws IOException {

    int lineLength = 0;
    byteArrayOutputStream.reset();
    int len = 0;

    while (true) {
      if (bufLength == -1) {
        return null;
      }

      int foundIndex = 0;

      if (offset == bufLength || (foundIndex = indexOf(buf, offset, bufLength, recordDelimiter)) == -1) {
        if (bufLength - offset > recordDelimiter.length) {
          lineLength += bufLength - offset - recordDelimiter.length;
          if (lineLength > Constants.MAX_RECORD_SIZE) {
            throw new IllegalArgumentException(
                Constants.ERROR_INDICATOR + "line big than 200M - please check record delimiter");
          }
          byteArrayOutputStream.write(buf, offset, bufLength - recordDelimiter.length - offset);
          offset = bufLength - recordDelimiter.length;
        }

        if (offset < bufLength) {
          System.arraycopy(buf, offset, buf, 0, bufLength - offset);
        }

        bufLength = bufLength - offset;
        offset = 0;

        len = is.read(buf, bufLength, buf.length - bufLength);
        if (len == -1) {
          if (lineLength + bufLength - offset > 0) {
            byteArrayOutputStream.write(buf, offset, bufLength - offset);
            bufLength = -1;
            readBytes += lineLength + bufLength - offset;
            return byteArrayOutputStream.toByteArray();
          } else {
            return null;
          }
        }
        bufLength = bufLength + len;
      } else {
        byteArrayOutputStream.write(buf, offset, foundIndex - offset);
        offset = foundIndex + recordDelimiter.length;
        byte[] returnValue = byteArrayOutputStream.toByteArray();
        readBytes += returnValue.length + recordDelimiter.length;
        return returnValue;
      }
    }
  }

  public long getReadBytes() {
    return readBytes;
  }

  public void close() throws IOException {
    is.close();
  }

  public String getDetectedCharset() {
    return detectedCharset;
  }

  public static int indexOf(byte[] src, int offset, int length, byte[] search) {
    int index = offset;
    while (index <= length - search.length) {
      boolean find = true;
      for (int j = 0; j < search.length; j++) {
        if (src[index + j] != search[j]) {
          find = false;
          break;
        }
      }
      if (find) {
        return index;
      }
      ++index;
    }
    return -1;
  }

  /**
   * rule 1: if file has bom head, which is indicated by encoding != 0, InputStream should skip bom bytes
   * rule 2: 单个文件被切分成多个block时，为了保证数据的完整性，前一个block会读到行分隔符的偏移量大于block offset才会停止，
   * 这一行数据会归入到前一个block。相应的，后一个block会忽略第一个分隔符之前的数据，因为这一行的数据会已经在前一个block。
   * 一种特殊的情况是行分隔符有多个字符，且block恰好切分在某个行分隔符之间。这个前一个block读到这个分隔符就会停止。
   * 为了让后一个block识别出这个分隔符，而不是把下一个分隔符当作第一个分隔符导致数据丢失，后一个block需要从原始的startPos
   * 往前偏移recordDelimeter.length － 1
   */
  private void init() throws IOException {
    detectBomCharset();
    readBytes = 0;
    //TODO  using one input stream,but why only on thread failed?
    is = new BufferedInputStream(blockInfo.getFileInputStream());
    startPos = 0;
    if (blockInfo.getStartPos() == 0L) {
      if (detectedCharset != null) {
        startPos = bomBytes;
        if (is.skip(startPos) != startPos) {
          throw new IOException(String.format("block %s failed to seek to position %s",
                                              blockInfo.getBlockId(), startPos));
        }
      }
      if (ignoreHeader) {
        readLine();
      }
    } else {
      startPos = blockInfo.getStartPos() - (recordDelimiter.length - 1);
      if (is.skip(startPos) != startPos) {
        throw new IOException(String.format("block %s failed to seek to position %s",
                                            blockInfo.getBlockId(), startPos));
      }
      readLine();
    }
    isLastLine = false;
  }

  /**
   *
   * http://www.unicode.org/unicode/faq/utf_bom.html BOMs: 00 00 FE FF = UTF-32, big-endian FF FE 00
   * 00 = UTF-32, little-endian EF BB BF = UTF-8, FE FF = UTF-16, big-endian FF FE = UTF-16,
   * little-endian
   *
   ***/
  /**
   * Read four bytes and check for BOM marks.
   */
  private void detectBomCharset() throws IOException {
    InputStream internalIs = blockInfo.getFileInputStream();
    try {
      byte bom[] = new byte[4];
      int n = internalIs.read(bom, 0, bom.length);

      if ((n >= 4) && (bom[0] == (byte) 0x00) && (bom[1] == (byte) 0x00) &&
          (bom[2] == (byte) 0xFE) && (bom[3] == (byte) 0xFF)) {
        detectedCharset = "UTF-32BE";
        bomBytes = 4;
      } else if ((n >= 4) && (bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE) &&
                 (bom[2] == (byte) 0x00) && (bom[3] == (byte) 0x00)) {
        detectedCharset = "UTF-32LE";
        bomBytes = 4;
      } else if ((n >= 3) && (bom[0] == (byte) 0xEF) && (bom[1] == (byte) 0xBB) &&
                 (bom[2] == (byte) 0xBF)) {
        detectedCharset = "UTF-8";
        bomBytes = 3;
      } else if ((n >= 2) && (bom[0] == (byte) 0xFE) && (bom[1] == (byte) 0xFF)) {
        detectedCharset = "UTF-16BE";
        bomBytes = 2;
      } else if ((n >= 2) && (bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE)) {
        detectedCharset = "UTF-16LE";
        bomBytes = 2;
      } else {
        // Unicode BOM mark not found, unread all bytes
        detectedCharset = null;
        bomBytes = 0;
      }
    } finally {
      IOUtils.closeQuietly(internalIs);
    }
  }
}

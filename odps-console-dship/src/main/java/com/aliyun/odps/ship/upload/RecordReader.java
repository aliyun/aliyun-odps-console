package com.aliyun.odps.ship.upload;


import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.aliyun.odps.ship.common.BlockInfo;


public abstract class   RecordReader {
  protected long startPos = 0;
  protected long readBytes = 0;
  protected BlockInfo blockInfo;
  protected String detectedCharset;
  protected int bomBytes;

  public RecordReader(BlockInfo info) {
    this.blockInfo = info;
  }

  public long getReadBytes() {
    return readBytes;
  }

  public abstract byte[][] readTextRecord() throws IOException;

  public abstract String getCurrentLine();

  public abstract  void close() throws IOException;

  public String getDetectedCharset() {
    return detectedCharset;
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
  protected void detectBomCharset() throws IOException {
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

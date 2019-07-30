package com.aliyun.odps.ship.upload;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.aliyun.odps.ship.common.BlockInfo;

import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.Util;
import com.csvreader.CsvReader;
import com.google.common.collect.Lists;

public class CsvRecordReader extends RecordReader {

  private CsvReader csvReader;
  private String currentLine;
  private String charset;
  private boolean ignoreHeader;

  public CsvRecordReader(BlockInfo info, String charset, boolean ignoreHeader) throws IOException {
    super(info);
    this.charset = charset;
    this.ignoreHeader = ignoreHeader;

    init();
  }

  public String getCurrentLine() {
    return currentLine;
  }


  public byte[][] readTextRecord() throws IOException {

    if (csvReader.readRecord()) {
      ArrayList<byte[]> line = Lists.newArrayList();

      currentLine = csvReader.getRawRecord();
      if (currentLine.getBytes().length > Constants.MAX_RECORD_SIZE) {
        throw new IllegalArgumentException(
            Constants.ERROR_INDICATOR + "line bigger than 200M - please check the csv file.");
      }

      String[] values = csvReader.getValues();
      for (String v : values) {
        line.add(v.getBytes(charset));
        readBytes += v.getBytes().length;
      }

      return line.toArray(new byte[line.size()][]);
    }

    return null;
  }

  protected void close() throws IOException {
    csvReader.close();
  }

  private void init() throws IOException {
    detectBomCharset();

    charset = detectedCharset != null ? detectedCharset
                                      : (Util.isIgnoreCharset(charset) ? Constants.REMOTE_CHARSET
                                                                       : charset);

    InputStream is = new BufferedInputStream(blockInfo.getFileInputStream());

    if (bomBytes != 0) {
      if (is.skip(bomBytes) != bomBytes) {
        throw new IOException(String.format("block %s failed to seek to position %s",
                                            blockInfo.getBlockId(), bomBytes));
      }
    }

    csvReader = new CsvReader(is, Charset.forName(charset));
    csvReader.setSafetySwitch(false);

    if (ignoreHeader) {
      csvReader.readHeaders();
    }
  }
}

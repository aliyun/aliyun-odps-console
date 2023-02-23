package com.aliyun.odps.ship.download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.Util;
import com.csvreader.CsvWriter;
import com.google.common.collect.Maps;

public class CsvRecordWriter extends RecordWriter {
  private CsvWriter csvWriter;
  private Charset charset;


  public CsvRecordWriter(File file, String cs) throws FileNotFoundException {
    super(file);

    String charsetName = Util.isIgnoreCharset(cs) ? Constants.REMOTE_CHARSET : cs;
    this.charset = Charset.forName(charsetName);
    this.csvWriter = new CsvWriter(os, ',', this.charset);
  }

  @Override
  public void write(byte[][] line, List<byte[]> ptVals) throws IOException {
    for (byte [] value : line) {
      csvWriter.write(new String(value, charset), true);
    }

    for (byte[] ptVal : ptVals) {
      csvWriter.write(new String(ptVal, charset), true);
    }

    csvWriter.endRecord();
  }

  @Override
  public void close() throws IOException {
    csvWriter.close();
  }
}

package com.aliyun.odps.ship.download;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.output.CountingOutputStream;

public abstract class RecordWriter {
  private final int BUFFER_SIZE = 8 * 1024 * 1024;

  protected CountingOutputStream os;

  public RecordWriter(File file) throws FileNotFoundException {
    this.os = new CountingOutputStream(new BufferedOutputStream(new FileOutputStream(file), BUFFER_SIZE));
  }

  public abstract void write(byte[][] line, List<byte[]> ptVals) throws IOException;
  public abstract void close() throws IOException;
  public long getWrittedBytes() {
    return os.getByteCount();
  }
}

package com.aliyun.openservices.odps.console.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Created by dongxiao on 2019/12/20.
 */
public class LocalCacheUtils {

  public static class CacheItem {
    public Long attachTime;
    public String sessionId;
    public String projectName;
    public String sessionName;

    public CacheItem(String sessionId, Long attachTime, String projectName, String sessionName) {
      this.sessionId = sessionId;
      this.attachTime = attachTime;
      this.projectName = projectName;
      this.sessionName = sessionName;
    }
    /* override */
    public String toString() {
      return gson.toJson(this, CacheItem.class);
    }
  }

  private static final String cacheFileName = ".odpscmd_interactive_cache";
  // lock this file after started, so nobody else can start in interactive mode
  private static final String cacheLockFileName = ".odpscmd_interactive_cache_lock";
  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static FileLock lock = null;
  private static String cacheDir;

  public static void setCacheDir(String path) {
    cacheDir = path;
  }

  public static String getCacheFileDir() {
    return new File(cacheDir).getAbsoluteFile().getParent() + "/";
  }

  public static String getCacheFile() {
    return cacheDir + cacheFileName;
  }

  public static String getCacheLockFile() {
    return cacheDir + cacheLockFileName;
  }

  public static FileLock lockCache(String path) throws IOException {
    File file = new File(path);
    if (!file.exists()) {
      file.createNewFile();
    }
    FileChannel fileChannel = new FileOutputStream(path).getChannel();
    FileLock lock = fileChannel.tryLock();
    return lock;
  }

  public static void clearCache() {
    new File(getCacheFile()).delete();
    new File(getCacheLockFile()).delete();
  }

  public static void checkLock() throws IOException {
    if (lock == null) {
      lock = lockCache(getCacheLockFile());
    }
    if (lock == null) {
      throw new IOException("Odpscmd has beed locked by another interactive mode progress.");
    }
  }

  public static void writeCache(CacheItem cache) throws IOException {
    try {
      FileWriter writer = new FileWriter(getCacheFile());
      writer.write(gson.toJson(cache));
      writer.close();
    } catch (IOException e) {
      throw e;
    }
  }

  public static CacheItem readCache() throws IOException {
    CacheItem cache = null;
    // when load from file, check if it is locked
    // if not locked. lock it until this cmd exists
    checkLock();
    try {
      String result = "";
      InputStream is = new FileInputStream(getCacheFile());
      String line;
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      while ((line = reader.readLine()) != null) {
        result += line;
        result+= "\n";
      }
      reader.close();
      is.close();
      cache = gson.fromJson(result, CacheItem.class);
    } catch (FileNotFoundException e) {
      // ignore
    } catch(JsonSyntaxException e) {
      // invalid file content, ignore and just return null
    } catch (IOException e) {
      throw e;
    }
    return cache;
  }
}

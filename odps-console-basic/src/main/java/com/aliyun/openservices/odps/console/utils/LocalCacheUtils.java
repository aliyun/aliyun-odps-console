package com.aliyun.openservices.odps.console.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.codec.digest.DigestUtils;

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

  private static boolean multiAttachSessionMode = false;
  private static int MAX_CACHE_COUNT = 5;
  private static final String cacheDirPrefix = ".session/";
  private static final String cacheFileName = ".odpscmd_interactive_cache";
  // lock this file after started, so nobody else can start in interactive mode
  private static final String cacheLockFileName = ".odpscmd_interactive_cache_lock";
  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
  private static FileLock gLock = null;
  private static String cacheDir;
  private static String configDir;

  public static void setCacheDir(String config, String sessionHash) {
    configDir = config;
    cacheDir = configDir + cacheDirPrefix + sessionHash + "/";
  }

  public static void enableMultiAttachSessionMode(Long maxAttachCount) {
    multiAttachSessionMode = true;
    MAX_CACHE_COUNT = maxAttachCount.intValue();
  }

  public static void setCacheDir(String configFile, String endpoint, String projectName, String accessId) throws IOException {
    if (!multiAttachSessionMode) {
      String configKey = endpoint + "_" + projectName + "_" + accessId;
      String sessionHash = DigestUtils.md5Hex(configKey).toUpperCase();
      setCacheDir(new File(configFile).getAbsoluteFile().getParent() + "/", sessionHash);
    } else {
      configDir = new File(configFile).getAbsoluteFile().getParent() + "/";
      File[] allCache = listAllCacheDir();
      // should create a new cache dir
      if (allCache.length < MAX_CACHE_COUNT) {
        Double random = Math.random();
        String configKey = endpoint + "_" + projectName + "_" + accessId + "_" + System.currentTimeMillis() + random.toString();
        String sessionHash = DigestUtils.md5Hex(configKey).toUpperCase();
        setCacheDir(new File(configFile).getAbsoluteFile().getParent() + "/", sessionHash);
      } else { // should found a idle dir and reuse session
        boolean found = false;
        for (File tmpCacheDir : allCache) {
          if (tmpCacheDir.isDirectory()) {
            String dir = tmpCacheDir.getAbsolutePath() + "/";
            String lockPath = dir + cacheLockFileName;
            File lockFile = new File(lockPath);
            if (lockFile.exists()) {
              FileChannel fileChannel = new FileOutputStream(lockPath).getChannel();
              FileLock lock = fileChannel.tryLock();
              if (lock == null) {
                // this cache is in using
                System.out.println("AttachSession is already in using:" + lockPath);
              } else {
                // this cache is idle, can reuse
                System.out.println("AttachSession is reusing:" + lockPath);
                cacheDir = tmpCacheDir.getAbsolutePath() + "/";
                lock.release();
                fileChannel.close();
                found = true;
                break;
              }
            }
          }
        }
        if (!found) {
          throw new IOException("Attach session has reaches max count:" + MAX_CACHE_COUNT);
        }
      }
    }
  }

  public static String getCacheFile() {
    return cacheDir + cacheFileName;
  }

  public static String getCacheLockFile() {
    return cacheDir + cacheLockFileName;
  }

  public static FileLock lockCache(String path) throws IOException {
    File sessionDir = new File(cacheDir);
    if (!sessionDir.exists()) {
      sessionDir.mkdirs();
    }
    File file = new File(path);
    if (!file.exists()) {
      file.createNewFile();
    }

    FileChannel fileChannel = new FileOutputStream(path).getChannel();
    FileLock lock = fileChannel.tryLock();
    return lock;
  }

  public static File[] listAllCacheDir() {
    String cacheBasePath = configDir + cacheDirPrefix;
    File cacheBaseDir = new File(cacheBasePath);
    if (!cacheBaseDir.exists()) {
      cacheBaseDir.mkdirs();
    }
    File[] allCache = cacheBaseDir.listFiles();
    return allCache;
  }

  public static void checkAndClearAllUselessCache() throws IOException {
    File[] allCache = listAllCacheDir();
    if (allCache.length < MAX_CACHE_COUNT) {
      return;
    }
    for (File tmpCacheDir : allCache) {
      if (tmpCacheDir.isDirectory()) {
        String dir = tmpCacheDir.getAbsolutePath() + "/";
        if (cacheDir.equals(dir)) {
          continue;
        }
        boolean needClean = true;
        String lockPath = dir + cacheLockFileName;
        File lockFile = new File(lockPath);
        if (lockFile.exists()) {
          FileChannel fileChannel = new FileOutputStream(lockPath).getChannel();
          FileLock lock = fileChannel.tryLock();
          if (lock == null) {
            // this cache is in using
            needClean = false;
          }
        }
        if (needClean) {
          new File(dir + cacheFileName).delete();
          new File(lockPath).delete();
          new File(dir).delete();
        }
      }
    }
  }

  public static void clearCache() {
    try {
      new File(getCacheFile()).delete();
      new File(getCacheLockFile()).delete();
      new File(cacheDir).delete();
    } catch (Exception e) {
      //ignore exception
    }
  }

  public static void checkLock() throws IOException {
    if (gLock == null) {
      gLock = lockCache(getCacheLockFile());
    }
    if (gLock == null) {
      throw new IOException("Odpscmd has beed locked by another interactive mode progress.");
    }
    if (!multiAttachSessionMode) {
      try {
        checkAndClearAllUselessCache();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  public static void writeCache(CacheItem cache) throws IOException {
    try {
      File sessionDir = new File(cacheDir);
      if (!sessionDir.exists()) {
        sessionDir.mkdirs();
      }
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

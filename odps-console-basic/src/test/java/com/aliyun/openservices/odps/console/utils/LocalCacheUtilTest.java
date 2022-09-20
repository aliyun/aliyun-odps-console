package com.aliyun.openservices.odps.console.utils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

public class LocalCacheUtilTest {

  @AfterClass
  public static void clear() {
    LocalCacheUtils.clearCache();
  }

  private void writeTestCache(String path, String hash) throws IOException {
    LocalCacheUtils.setCacheDir(path, hash);
    LocalCacheUtils.CacheItem cache =
        new LocalCacheUtils.CacheItem("testsessionid", System.currentTimeMillis()/1000,"test","public.default.test");
    try {
      LocalCacheUtils.writeCache(cache);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testGetRootPath() throws IOException {
    File baseDir = new File("session_test");
    String basePath = baseDir.getAbsolutePath() + "/";
    for (int i = 0; i < 5; ++i) {
      writeTestCache(basePath, "test_hash" + i);
    }
    LocalCacheUtils.setCacheDir(basePath, "test_hash");
    String cacheFile = LocalCacheUtils.getCacheFile();
    System.out.println(new File(cacheFile).getAbsolutePath());
    Assert.assertEquals(basePath + ".session/test_hash/.odpscmd_interactive_cache", cacheFile);

    LocalCacheUtils.CacheItem cacheResult = null;
    try {
      cacheResult = LocalCacheUtils.readCache();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    Assert.assertNull(cacheResult);

    LocalCacheUtils.CacheItem cache =
        new LocalCacheUtils.CacheItem("testsessionid", System.currentTimeMillis()/1000,"test","public.default.test");
    try {
      LocalCacheUtils.writeCache(cache);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      cacheResult = LocalCacheUtils.readCache();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
    Assert.assertNotNull(cacheResult);
    System.out.println(cacheResult.toString());
    Assert.assertEquals(cacheResult.attachTime, cache.attachTime);
    Assert.assertEquals(cacheResult.sessionId, cache.sessionId);
    Assert.assertEquals(cacheResult.projectName, cache.projectName);
    Assert.assertEquals(cacheResult.sessionName, cache.sessionName);

    LocalCacheUtils.clearCache();
    File cacheDir = new File(basePath + ".session/test_hash");
    Assert.assertTrue(!cacheDir.exists());
  }
}

package com.aliyun.openservices.odps.console.utils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LocalCacheUtilTest {

  @AfterClass
  public static void clear() {
    LocalCacheUtils.clearCache();
  }

  @Test
  public void testGetRootPath() {
    String cacheFile = LocalCacheUtils.getCacheFile();
    System.out.println(cacheFile);

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
  }
}

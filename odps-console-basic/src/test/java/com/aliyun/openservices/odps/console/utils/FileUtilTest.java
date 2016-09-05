package com.aliyun.openservices.odps.console.utils;

import org.junit.Test;

import org.junit.Assert;

/**
 * Created by zhenhong.gzh on 16/7/18.
 */
public class FileUtilTest {
  @Test
  public void testExpandUserHomeInPath() {
    String path = "~/test_dir/file";

    Assert.assertEquals(System.getProperty("user.home") + "/test_dir/file",
                        FileUtil.expandUserHomeInPath(path));

    path = "~";
    Assert.assertEquals(System.getProperty("user.home"), FileUtil.expandUserHomeInPath(path));

    path = "~user";
    Assert.assertEquals(path, FileUtil.expandUserHomeInPath(path));

    path = "/test/test_dir";
    Assert.assertEquals(path, FileUtil.expandUserHomeInPath(path));
  }
}

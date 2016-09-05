package com.aliyun.openservices.odps.console.resource;

import org.junit.Test;
import org.junit.Assert;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by ruibo.lirb on 2015-11-24.
 */
public class GetResourceCommandTest {

  @Test
  public void testMultiSpaceAfterResourceName() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    GetResourceCommand cmd = GetResourceCommand.parse("get resource foo  ./bar", ctx);
    Assert.assertEquals(cmd.resourceName, "foo");

    cmd = GetResourceCommand.parse("get resource foo.txt   ./bar.txt", ctx);
    Assert.assertEquals(cmd.resourceName, "foo.txt");
  }

}

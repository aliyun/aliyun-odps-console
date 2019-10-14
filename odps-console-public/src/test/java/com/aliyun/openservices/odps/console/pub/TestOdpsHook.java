package com.aliyun.openservices.odps.console.pub;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHook;

public class TestOdpsHook extends OdpsHook {
  public static int beforeCallCounter = 0;
  public static int afterCallCounter = 0;

  @Override
  public void before(Job job, Odps odps) throws OdpsException {
    System.err.println("TestOdpsHook: before is called");
    beforeCallCounter += 1;
  }

  @Override
  public void after(Instance instance, Odps odps) throws OdpsException {
    System.err.println("TestOdpsHook: after is called");
    afterCallCounter += 1;
  }
}

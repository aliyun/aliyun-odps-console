package com.aliyun.openservices.odps.console.output.state;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.utils.statemachine.State;

/**
 * Created by zhenhong.gzh on 16/8/25.
 */
public abstract class InstanceState implements State<InstanceStateContext> {

  @Override
  public abstract State run(InstanceStateContext context) throws OdpsException;
}

package com.aliyun.openservices.odps.console.output.state;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.utils.statemachine.State;

/**
 * Created by zhenhong.gzh on 16/8/24.
 */
public class InstanceCreated extends InstanceState {

  @Override
  public State run(InstanceStateContext context) throws OdpsException {
    context.printInstanceId();
    context.printLogview();
    context.setInstanceStartTime(System.currentTimeMillis());

    return new InstanceRunning();
  }

}

package com.aliyun.openservices.odps.console.utils.statemachine;

import com.aliyun.odps.OdpsException;

/**
 * Created by zhenhong.gzh on 16/8/24.
 */
public class DefaultStateManager<T extends StateContext> implements StateManager<T>{

  @Override
  public void start(T context, State initState) throws OdpsException {
    while (initState != State.END) {
      initState = initState.run(context);
    }
  }

}

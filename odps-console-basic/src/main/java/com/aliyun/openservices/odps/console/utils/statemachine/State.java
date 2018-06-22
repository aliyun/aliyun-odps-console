package com.aliyun.openservices.odps.console.utils.statemachine;

import com.aliyun.odps.OdpsException;

/**
 * Created by zhenhong.gzh on 16/8/24.
 */
public interface State<T extends StateContext> {
  State END = new State() {
    @Override
    public State run(StateContext context) throws OdpsException {
      throw new RuntimeException("END state cannot be run.");
    }
  };

  State run(T context) throws OdpsException;
}

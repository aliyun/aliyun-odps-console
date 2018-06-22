package com.aliyun.openservices.odps.console.utils.statemachine;

/**
 * Created by zhenhong.gzh on 16/8/24.
 */
public interface StateManager<T extends StateContext> {
  void start(T context, State initState) throws Exception;
}

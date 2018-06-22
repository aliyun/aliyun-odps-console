package com.aliyun.openservices.odps.console.commands;

import com.aliyun.openservices.odps.console.ExecutionContext;

/**
 * 继承自 DirectCommand 的命令在 InteractiveCommand 交互式模式下不做 use project 检查
 *
 * Created by ruibo.lirb on 2016-8-12.
 */
public abstract class DirectCommand extends AbstractCommand {

  public DirectCommand(String commandText,
                       ExecutionContext context) {
    super(commandText, context);
  }
}

package com.aliyun.openservices.odps.console.output.state;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.utils.statemachine.State;

/**
 * Instance 运行结束状态
 * 获取并检查 instance task 状态，获取 instance result 信息
 *
 *
 * Created by zhenhong.gzh on 16/8/25.
 */
public class InstanceTerminated extends InstanceState {
  @Override
  public State run(InstanceStateContext context) throws OdpsException {
    // 检查状态，若失败：直接抛异常
    try {
      checkTaskStatus(context);
    } catch (OdpsException e) {
      if (context.getInstance().isSync()) {
        // 同步任务，生成 logview 输出
        context.printLogview();
      }

      throw e;
    }

    // 若是同步 instance, 则结束状态机
    if (context.getInstance().isSync()) {
      return State.END;
    }

    // 若进度打印没有完成，则强行刷新进度至 100% 状态
    if (!context.isProgressReportFinish()) {
      InstanceProgressReporter reporter = new InstanceProgressReporter(context);
      reporter.printProgress(true);
    }

    return new InstanceSuccess();
  }

  public static void checkTaskStatus(InstanceStateContext context) throws OdpsException {
    Instance.TaskStatus taskStatus = context.getTaskStatus();

    if (Instance.TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
      // 如果不是跨集群，直接抛出异常
      throw new OdpsException(context.getInstance().getTaskResults().get(taskStatus.getName()));
    } else if (Instance.TaskStatus.Status.CANCELLED.equals(taskStatus.getStatus())) {
      throw new OdpsException("Task got cancelled");
    } else if (!Instance.TaskStatus.Status.SUCCESS.equals(taskStatus.getStatus())) {
      // #ODPS-43495
      throw new OdpsException(
          "Task not successfully terminated: status[" + taskStatus.getStatus() + "]");
    }
  }
}

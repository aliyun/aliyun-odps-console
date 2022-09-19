package com.aliyun.openservices.odps.console.output.state;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.utils.FormatUtils;
import com.aliyun.openservices.odps.console.utils.statemachine.State;

import sun.util.calendar.ZoneInfo;

/**
 * Instance 运行结束状态
 *
 * 这个状态主要负责判断任务是否成功，如果成功，则获取结果，如果失败，则 throw {@link OdpsException}
 *
 * 为了减少 Http request 的次数，我们通过获取结果的 API 来判断任务是否成功
 *
 * Created by zhenhong.gzh on 16/8/25.
 */
public class InstanceTerminated extends InstanceState {
  private static final String INSTANCE_NOT_TERMINATED = "InstanceNotTerminate";
  private static final String TASK_FAILED = "TaskFailed";

  @Override
  public State run(InstanceStateContext context) throws OdpsException {
    try {
      Task task = context.getTasks().get(0);
      if (isGetResultByInstanceTunnel(context, task)) {
        setTaskResultByInstanceTunnel(context);
      } else {
        setTaskResult(context);
      }
    } catch (OdpsException e) {
      // 同步任务执行成功时不需要打印 logview, 但执行失败时需要打印
      if (context.getInstance().isSync()) {
        // InstanceTerminated 为同步任务状态机起始状态, 因此没有打印过 logview, 需要在此打印 logview
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

  private void setTaskResult(InstanceStateContext context) throws OdpsException {
    Instance.InstanceResultModel.TaskResult taskResult =
        context.getInstance().getRawTaskResults().get(0);
    // The result contains error message when the task failed
    String resultStr = taskResult.getResult().getString();

    // If the service doesn't support optimized key-path, call getStatus()
    Instance.TaskStatus.Status taskStatus;
    if (!StringUtils.isNullOrEmpty(taskResult.getStatus())) {
      taskStatus = Instance.TaskStatus.Status.valueOf(taskResult.getStatus().toUpperCase());
    } else {
      taskStatus = context.getTaskStatus().getStatus();
    }

    if (Instance.TaskStatus.Status.FAILED.equals(taskStatus)) {
      // 如果不是跨集群，直接抛出异常
      throw new OdpsException(resultStr);
    } else if (Instance.TaskStatus.Status.CANCELLED.equals(taskStatus)) {
      throw new OdpsException("Task got cancelled");
    } else if (!Instance.TaskStatus.Status.SUCCESS.equals(taskStatus)) {
      // #ODPS-43495
      throw new OdpsException(
          "Task not successfully terminated: status[" + taskStatus + "]");
    }

    List<String> result = new ArrayList<>();
    result.add(resultStr);
    context.setResult(result.iterator());
  }

  private void setTaskResultByInstanceTunnel(InstanceStateContext context)
      throws OdpsException {
    Long sessionMaxRow = context.getExecutionContext().getInstanceTunnelMaxRecord();
    String tunnelEndpoint = context.getExecutionContext().getTunnelEndpoint();
    URI tunnelUri;
    try {
      tunnelUri = StringUtils.isNullOrEmpty(tunnelEndpoint) ? null : new URI(tunnelEndpoint);
    } catch (URISyntaxException e) {
      throw new OdpsException("Illegal tunnel endpoint: " + tunnelEndpoint
                              + "please check the config.", e);
    }
    DateFormat dateFormat = (DateFormat) FormatUtils.DATETIME_FORMAT.clone();
    if (!StringUtils.isNullOrEmpty(context.getExecutionContext().getSqlTimezone())) {
      try {
        ZoneId zoneId = ZoneId.of(context.getExecutionContext().getSqlTimezone());
        dateFormat
            .setTimeZone(TimeZone.getTimeZone(zoneId));
      } catch (Exception e) {
        throw new OdpsException(
            "Failed to get TimeZone, " + e.getMessage(), e);
      }
    }

    try {
      ResultSet resultSet = SQLTask.getResultSet(
          context.getInstance(),
          context.getRunningTaskName(),
          sessionMaxRow,
          false,
          tunnelUri);
      Iterator<String> result = new FormatUtils.FormattedResultSet(
          resultSet,
          FormatUtils.GSON,
          dateFormat);
      context.setResult(result);
    } catch (TunnelException e) {
      // Tunnel may throw the following exceptions when task failed. In this case, we should
      // get the error message by API
      if (INSTANCE_NOT_TERMINATED.equals(e.getErrorCode())
          || TASK_FAILED.equals(e.getErrorCode())) {
        setTaskResult(context);
      } else {
        throw e;
      }
    } catch (OdpsException e) {
      throw new OdpsException("Failed to get query result by instance tunnel, " +
                              e.getMessage(),  e);
    }
  }

  private static boolean isGetResultByInstanceTunnel(InstanceStateContext context, Task task) {
    return task instanceof SQLTask
           && ((SQLTask) task).getQuery().toUpperCase().matches("^SELECT[\\s\\S]*")
           && context.getExecutionContext().isUseInstanceTunnel();
  }
}

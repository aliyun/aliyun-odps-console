package com.aliyun.openservices.odps.console.output.state;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.statemachine.StateContext;

import jline.console.UserInterruptException;

/**
 * Created by zhenhong.gzh on 16/8/24.
 */
public class InstanceStateContext implements StateContext {

  public static final long STATUS_QUERY_INTERVAL = TimeUnit.SECONDS.toMillis(5);

  private Instance instance;
  private List<Instance.StageProgress> taskProgress;
  private Instance.TaskSummary summary;
  private String result;
  private Odps odps;
  private ExecutionContext context;
  private List<Task> tasks;
  private boolean isTerminate = false;
  private boolean isReportFinish = false;
  private Instance.TaskStatus taskStatus;
  private int repositionLines = 0;
  private long startTime;
  private final byte[] lock = new byte[0];

  public InstanceStateContext(Odps odps, Instance instance, ExecutionContext context)
      throws OdpsException {
    this.odps = odps;
    this.instance = instance;
    this.tasks = instance.getTasks();
    this.context = context;
  }

  public List<Task> getTasks() {
    return tasks;
  }

  public Instance getInstance() {
    return instance;
  }

  public void setInstance(Instance instance) {
    this.instance = instance;
  }

  public ExecutionContext getExecutionContext() {
    return context;
  }

  public Odps getOdps() {
    return odps;
  }

  public List<Instance.StageProgress> getTaskProgress() {
    return taskProgress;
  }

  public void setTaskProgress(List<Instance.StageProgress> taskProgress) {
    this.taskProgress = taskProgress;
  }

  public Instance.TaskSummary getSummary() {
    return summary;
  }

  public void setSummary(Instance.TaskSummary summary) {
    this.summary = summary;
  }

  public String getResult() throws OdpsException {
    if (result == null) {
      result = getInstance().getTaskResults().get(getTaskStatus().getName());
    }

    return result;
  }

  public String getRunningTaskName() {
    String runningTaskName = null;
    if (tasks != null && tasks.size() == 1) {
      //一个task是当前最多的情况，为了节省一次getTaskStatus的requst，缓存一下，这个请求访问很频繁
      runningTaskName = tasks.get(0).getName();
    } else {
      try {
        Map<String, Instance.TaskStatus> taskStatuses = instance.getTaskStatus();
        for (String name : taskStatuses.keySet()) {
          //只report第一个找到的running的task
          if (Instance.TaskStatus.Status.RUNNING.equals(taskStatuses.get(name).getStatus())) {
            runningTaskName = name;
            break;
          }
        }
      } catch (Exception e) {
        //如果取不到, 本次不report出去
      }
    }

    return runningTaskName;
  }
  
  public void setProgressReportFinish(boolean isFinished) {
    isReportFinish = isFinished;
  }

  public boolean isProgressReportFinish() {
    return isReportFinish;
  }

  public Instance.TaskStatus getTaskStatus() throws OdpsException {
    if (taskStatus == null) {
      Map<String, Instance.TaskStatus> taskStatuses = getInstance().getTaskStatus();

      // 当前task名称
      String cTaskName = null;
      for (String taskName : taskStatuses.keySet()) {
        Instance.TaskStatus tStatus = taskStatuses.get(taskName);
        if (Instance.TaskStatus.Status.FAILED.equals(tStatus.getStatus())) {
          cTaskName = taskName;
        } else if (Instance.TaskStatus.Status.RUNNING.equals(tStatus.getStatus())) {
          cTaskName = taskName;
        }
      }

      if (cTaskName == null) {
        cTaskName = getTasks().get(getTasks().size() - 1).getName();
      }

      taskStatus = taskStatuses.get(cTaskName);
      if (taskStatus == null) {
        throw new OdpsException("task status unknown. taskname=" + cTaskName);
      }
    }

    return taskStatus;

  }

  public void setRepositionLines(int lines) {
    repositionLines = lines;
  }

  public int getRepositionLines() {
    return repositionLines;
  }

  public long getInstanceStartTime() {
    return startTime;
  }

  public void setInstanceStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void printLogview() {
    String logviewUrl = ODPSConsoleUtils.generateLogView(odps, instance, context);
    if (!StringUtils.isNullOrEmpty(logviewUrl)) {
      context.getOutputWriter().writeError("Log view:");
      context.getOutputWriter().writeError(logviewUrl);
    }
  }

  public void printInstanceId() {
    context.getOutputWriter().writeResult("");
    context.getOutputWriter().writeError("ID = " + instance.getId());
  }

  public void setInstanceTerminate() {
    synchronized (lock) {
      isTerminate = true;
      try {
        lock.notify();
      } catch (IllegalMonitorStateException e) {
        // ignore
      }
    }
  }


  public boolean isInstanceTerminate() {
    waitForStatusChecker();

    boolean terminateFlag;
    synchronized (lock) {
      terminateFlag = isTerminate;
    }

    return terminateFlag;
  }

  private void waitForStatusChecker() {
    synchronized (lock) {
      if (!isTerminate) {
        try {
          lock.wait(STATUS_QUERY_INTERVAL);
        } catch (InterruptedException e) {
          throw new UserInterruptException("interrupted while thread sleep");
        }
      }
    }
  }
}

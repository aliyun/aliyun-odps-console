package com.aliyun.openservices.odps.console.output.state;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.fusesource.jansi.Ansi;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.output.InPlaceUpdates;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * Created by zhenhong.gzh on 17/7/5.
 */
public class InstanceProgressReporter {

  /* in-place progress update related variables */
  private static final int NAME_COLUMN_WIDTH = 26;
  private static final int SEPARATOR_WIDTH = InPlaceUpdates.MIN_TERMINAL_WIDTH;
  private static final String SEPARATOR = new String(new char[SEPARATOR_WIDTH]).replace("\0", "-");
  private static final String HEADER_FORMAT = "%26s %13s  %5s  %9s  %7s  %7s  %6s";
  private static final String STAGE_FORMAT = "%-26s %13s  %5s  %9s  %7s  %7s  %6s";
  private static final String FOOTER_FORMAT = "%-15s  %-30s %-4s  %-25s";
  private static final String HEADER = String.format(HEADER_FORMAT,
                                                     "STAGES", "STATUS", "TOTAL",
                                                     "COMPLETED", "RUNNING", "PENDING", "BACKUP");
  private static final NumberFormat secondsFormat = new DecimalFormat("#0.00");

  private static final PrintStream out = System.err;
  private static final int progressBarChars = 30;

  private int count = 0;
  private boolean isWarned = false;

  private String taskName;

  // complete stage name
  private Set<String> completedStageNames = new HashSet<String>();

  private InstanceStateContext context;
  private boolean queueing = true;
  List<Instance.StageProgress> stages = null;

  public InstanceProgressReporter(InstanceStateContext context) {
    this.context = context;
  }

  public void report() {
    reportWarnings();
    printQueueingMessage();

    while (!context.isInstanceTerminate()) {
      try {
        taskName = context.getRunningTaskName();
        if (!StringUtils.isNullOrEmpty(taskName)) {
          stages = context.getInstance().getTaskProgress(taskName);

          if (queueing) {
            reportQueueing();
          }

          if (stages != null && !stages.isEmpty()) {
            context.setTaskProgress(stages);
            reportProgress();
          }
        }
      } catch (Exception ignore) {
        // 如果拿进度出错，重复拿
      }

      ODPSConsoleUtils.checkThreadInterrupted();
    }
  }

  private void reportWarnings() {
    if (!isWarned) {
      try {
        printSqlWarnings(taskName);
      } catch (OdpsException ingore) {

      }

      isWarned = true;
    }
  }

  private void reportQueueing() throws OdpsException {
    if (stages != null && !stages.isEmpty()) {
      // progress 不为空，表示作业已经被调度，开始运行
      context.setRepositionLines(0);
      queueing = false;

      return;
    }

    printQueueingMessage();
  }

  private void printSqlWarnings(String taskName)
      throws OdpsException {
    List<String> warnings = SQLTask.getSqlWarning(context.getInstance(), taskName);

    if (warnings != null && !warnings.isEmpty()) {
      if (InPlaceUpdates.isUnixTerminal() && context.getRepositionLines() > 0) {
        InPlaceUpdates.rePositionCursor(out, context.getRepositionLines());
        InPlaceUpdates.reprintLine(out, "SQL Warnings:");
      } else {
        context.getExecutionContext().getOutputWriter().writeError("SQL Warnings:");
      }

      for (String warning : warnings) {
        context.getExecutionContext().getOutputWriter().writeError(warning);
      }

      context.setRepositionLines(0);
    }
  }

  private void printQueueingMessage() {
    if (InPlaceUpdates.isUnixTerminal()) {
      int line = context.getRepositionLines();
      if (line > 0) {
        InPlaceUpdates.rePositionCursor(out, line);
      }
      final String dots = new String(new char[count++ % 5 + 1]).replace("\0", ".");

      String message = "Job Queueing" + dots;
      InPlaceUpdates.reprintLine(out, message);

      context.setRepositionLines(1);
    } else {
      context.getExecutionContext().getOutputWriter().writeError( "Job Queueing...");
    }

  }


  private void reportProgress() throws OdpsException {
    printProgress(false);
  }


  public void printProgress(boolean isTerminate) {
    if (context.getTaskProgress() == null || context.getTaskProgress().isEmpty()) {
      return;
    }

    List<Instance.StageProgress> stageProgresses = context.getTaskProgress();
    if (InPlaceUpdates.isUnixTerminal()) {
      printProgressesInPlace(stageProgresses, isTerminate);
    } else {
      context.getExecutionContext().getOutputWriter().writeError(
          getProgressFormattedString(stageProgresses, isTerminate));
    }
    // 纪录当前进度信息, 是否为完成状态
    context.setProgressReportFinish(completedStageNames.size() == stageProgresses.size());
  }

  private String getProgressFormattedString(List<Instance.StageProgress> stages,
                                            boolean forceComplete) {
    StringBuilder result = new StringBuilder();

    SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String dateString = sim.format(new Date());
    result.append(dateString + ' ');
    for (Instance.StageProgress stage : stages) {
      int runningWorkers = forceComplete ? 0 : stage.getRunningWorkers();
      int terminateWorkers = forceComplete ? stage.getTotalWorkers() : stage.getTerminatedWorkers();
      Instance.StageProgress.Status status =
          forceComplete ? Instance.StageProgress.Status.TERMINATED : getStatus(stage);
      result.append(String.format("%s:%s/%s/%s%s%s", stage.getName(), runningWorkers,
                                  terminateWorkers, stage.getTotalWorkers(),
                                  stage.getBackupWorkers() > 0 ? "(+" + stage.getBackupWorkers()
                                                                 + " backups)" : "",
                                  status == null ? "\t" : "[" + status + "]\t"));

      if (status == Instance.StageProgress.Status.TERMINATED && !completedStageNames
          .contains(stage.getName())) {
        completedStageNames.add(stage.getName());
      }
    }

    return result.toString();
  }

  private Instance.StageProgress.Status getStatus(Instance.StageProgress stage) {
    try {
      return stage.getStatus();
    } catch (IllegalArgumentException ignore) {
      // to deal with some special tasks with illegal status value, such as PLTask
      return null;
    }
  }

  private void printProgressesInPlace(List<Instance.StageProgress> stageprogresses,
                                      boolean forceComplete) {
    StringBuilder reportBuffer = new StringBuilder();
    int sumComplete = 0;
    int sumTotal = 0;
    int idx = 0;
    int maxProgress = stageprogresses.size();
    int lines = context.getRepositionLines();

    if (lines > 0) {
      InPlaceUpdates.rePositionCursor(out, lines);
      InPlaceUpdates.resetForward(out);
      lines = 0;
    }

    // print header
    // -------------------------------------------------------------------------------
    //          STAGES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  BACKUP
    // -------------------------------------------------------------------------------
    InPlaceUpdates.reprintLine(out, SEPARATOR);
    lines++;
    InPlaceUpdates.reprintLineWithColorAsBold(out, HEADER, Ansi.Color.CYAN);
    lines++;
    for (Instance.StageProgress progress : stageprogresses) {
      idx++;
      final String name = progress.getName();
      // NOTE: getTotalWorkers() 不包含 backupWorkers，用户不需要感知 backupWorkers 这件事情
      // 大部分情况下 getBackupWorkers() 返回 0qu
      final int backup = progress.getBackupWorkers();
      final int total = progress.getTotalWorkers();
      final int all = backup + total;
      final int running = forceComplete ? 0 : progress.getRunningWorkers();
      final int completed = forceComplete ? all : progress.getTerminatedWorkers();
      final int pending = all - completed - running;

      final Instance.StageProgress.Status
          status =
          forceComplete ? Instance.StageProgress.Status.TERMINATED : getStatus(progress);
      String statusString = status != null ? status.toString() : "NULL";

      String nameWithProgress = getNameWithProgress(name, completed, total);
      // Stage 1 .......... mode  RUNNING      7          7        0        0       0       0     0
      String vertexStr = String.format(STAGE_FORMAT,
                                       nameWithProgress,
                                       statusString,
                                       total,
                                       completed,
                                       running,
                                       pending,
                                       backup);

      sumComplete += completed;
      sumTotal += total;

      // Mark the stage as Completed
      if (status == Instance.StageProgress.Status.TERMINATED && !completedStageNames
          .contains(name)) {
        completedStageNames.add(name);
      }

      reportBuffer.append(vertexStr);
      if (idx != maxProgress) {
        reportBuffer.append("\n");
      }
    }
    lines += InPlaceUpdates.reprintMultiLine(out, reportBuffer.toString());

    // -------------------------------------------------------------------------------
    // STAGES: 03/04            [=================>>-----] 86%  ELAPSED TIME: 1.71 s
    // -------------------------------------------------------------------------------
    InPlaceUpdates.reprintLine(out, SEPARATOR);
    lines++;
    final float progress = (sumTotal == 0) ? 0.0f : (float) sumComplete / (float) sumTotal;
    String
        footer =
        getFooter(stageprogresses.size(), completedStageNames.size(), progress,
                  context.getInstanceStartTime());
    InPlaceUpdates.reprintLineWithColorAsBold(out, footer, Ansi.Color.RED);
    lines++;
    InPlaceUpdates.reprintLine(out, SEPARATOR);
    lines++;

    context.setRepositionLines(lines);
  }

  // STAGES: 03/04            [==================>>-----] 86%  ELAPSED TIME: 1.71 s
  private String getFooter(int keySize, int completedSize, float progress, long startTime) {
    String verticesSummary = String.format("STAGES: %02d/%02d", completedSize, keySize);
    String progressBar = getInPlaceProgressBar(progress);
    final int progressPercent = (int) (progress * 100);
    String progressStr = "" + progressPercent + "%";
    float et = (float) (System.currentTimeMillis() - startTime) / (float) 1000;
    String elapsedTime = "ELAPSED TIME: " + secondsFormat.format(et) + " s";

    return String.format(FOOTER_FORMAT, verticesSummary, progressBar, progressStr, elapsedTime);
  }

  // [==================>>-----]
  private String getInPlaceProgressBar(float percent) {
    StringBuilder bar = new StringBuilder("[");
    int remainingChars = progressBarChars - 4;
    int completed = (int) (remainingChars * percent);
    int pending = remainingChars - completed;
    for (int i = 0; i < completed; i++) {
      bar.append("=");
    }
    bar.append(">>");
    for (int i = 0; i < pending; i++) {
      bar.append("-");
    }
    bar.append("]");
    return bar.toString();
  }

  // Stage 1 ..........
  private String getNameWithProgress(String s, int complete, int total) {
    String result = "";
    if (s != null) {
      float percent = total == 0 ? 0.0f : (float) complete / (float) total;
      // lets use the remaining space in name column as progress bar
      int spaceRemaining = NAME_COLUMN_WIDTH - s.length() - 1;
      String trimmedVName = s;

      if (s.length() > NAME_COLUMN_WIDTH) {
        trimmedVName = s.substring(0, NAME_COLUMN_WIDTH - 1);
        trimmedVName = trimmedVName + "..";
      }

      result = trimmedVName + " ";
      int toFill = (int) (spaceRemaining * percent);
      for (int i = 0; i < toFill; i++) {
        result += ".";
      }
    }
    return result;
  }

}

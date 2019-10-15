package com.aliyun.openservices.odps.console.output.state;

import java.util.concurrent.TimeUnit;

import org.jline.reader.UserInterruptException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.SignalUtil;
import com.aliyun.openservices.odps.console.utils.statemachine.State;

import sun.misc.SignalHandler;


/**
 * Created by zhenhong.gzh on 17/7/5.
 */
public class InstanceRunning extends InstanceState {

  class ReporterThread extends Thread {

    private InstanceStateContext context;

    public ReporterThread(InstanceStateContext context) {
      this.context = context;
    }

    @Override
    public void run() {
      try {
        InstanceProgressReporter reporter = new InstanceProgressReporter(context);
        reporter.report();
      } catch (Exception e) {
        //ignore
        context.getExecutionContext().getOutputWriter()
            .writeDebug(String.format("%s: %s, %s", "Instance progress reporter error",
                                      e.getMessage(), StringUtils.stringifyException(e)));
      }
    }

  }

  // continued status querying failed timeout
  private static final long STATUS_QUERY_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  private long lastUpdate;
  private long firstFailed = -1;
  private ReporterThread reporter = null;

  private void sleep() {
    long minPeriod = InstanceStateContext.STATUS_QUERY_INTERVAL;

    long costMillis = System.currentTimeMillis() - lastUpdate;
    if (costMillis < minPeriod) {
      try {
        Thread.sleep(minPeriod - costMillis);
      } catch (InterruptedException e) {
        throw new UserInterruptException("interrupted while thread sleep");
        // Just return if the thread is interrupted
      }
    }
  }

  @Override
  public State run(InstanceStateContext context) throws OdpsException {
    try {
      reporter = new ReporterThread(context);
      reporter.setDaemon(true);
      reporter.start();
    } catch (Exception e) {
      // ignore
    }

    // refine ctrl-c signal when interactive mode
    // cause getStatus in polling may be a blocking restful request
    if (context.getExecutionContext().isInteractiveMode()) {
      SignalHandler instanceRunningIntSignalHandler =
          SignalUtil.getInstanceRunningIntSignalHandler(Thread.currentThread());
      SignalUtil.registerSignalHandler("INT", instanceRunningIntSignalHandler);
    }

    try {
      polling(context);
    } finally {
      // restore default signal handler
      if (context.getExecutionContext().isInteractiveMode()) {
        SignalHandler defaultIntSignalHandler =
            SignalUtil.getDefaultIntSignalHandler(Thread.currentThread());
        SignalUtil.registerSignalHandler("INT", defaultIntSignalHandler);
      }
      stopReporter();
    }

    return new InstanceTerminated();
  }

  private void polling(InstanceStateContext context) {
    while (true) {
      ODPSConsoleUtils.checkThreadInterrupted();

      lastUpdate = System.currentTimeMillis();
      Instance.Status status = null;

      try {
        status = context.getInstance().getStatus(true); // cost time
        firstFailed = -1;
      } catch (ReloadException e) {
        if (firstFailed != -1 && (System.currentTimeMillis() - firstFailed >= STATUS_QUERY_TIMEOUT)) {
          throw e;
        }
        context.getExecutionContext().getOutputWriter()
            .writeDebug("Get instance status error: " + e.getMessage());

        if (firstFailed == -1) {
          firstFailed = System.currentTimeMillis();
        }
      }

      if (status == Instance.Status.TERMINATED) {
        context.setInstanceTerminate();
        break;
      }

      sleep();
    }
  }

  private void stopReporter() {
    if (reporter != null) {
      try {
        reporter.interrupt();
        reporter.join(InstanceStateContext.STATUS_QUERY_INTERVAL);
      } catch (Exception ignore) {

      }
    }
  }
}

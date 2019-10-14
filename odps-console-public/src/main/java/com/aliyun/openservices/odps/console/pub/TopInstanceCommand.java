package com.aliyun.openservices.odps.console.pub;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fusesource.jansi.Ansi;

import com.aliyun.odps.Instance;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.InPlaceUpdates;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

import org.jline.reader.UserInterruptException;

/**
 * Created by zhenhong.gzh on 17/5/16.
 */
public class TopInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"top", "instance"};

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HH:mm:ssZ");
  private static final int DEFAULT_NUMBER = 50;
  private static final int DEFAULT_DELAY = 3;
  private static final String SEPARATOR =
      new String(new char[InPlaceUpdates.MIN_TERMINAL_WIDTH]).replace("\0", "-");
  private static final String HEADER_FORMAT = "%s | %s | %s | %s | %s | %s | %s | %s | %s | %s";
  private static final String HEADER =
      String.format(HEADER_FORMAT, "ID", "Owner", "Type", "StartTime", "Progress", "Status",
                    "Priority",
                    "RuntimeUsage(CPU/MEM)", "TotalUsage(CPU/MEM)", "QueueingInfo(POS/LEN)");
  private static final PrintStream ERR = System.err;

  private static final String PROJECT_TAG = "p";
  private static final String LIMIT_TAG = "limit";
  private static final String DELAY_TAG = "d";
  private static final String ALL_TAG = "all";
  private static final String STATUS_TAG = "status";

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: top instance [options];\n"
                   + "       -p <project>                  specify the project\n"
                   + "       -status <running|suspended>   specify the instance status\n"
                   + "       -limit <number>               specify the maximum number of queueing instance to show, default 50\n"
                   + "       -d [<delay>]                  auto refresh every <delay> seconds, default 3\n"
                   + "       -all                          list all instances, not only yours");
  }

  private int nubmer = DEFAULT_NUMBER;

  private boolean onlyOwner = true;
  private Integer delay = null;
  private String projectName;
  private Instance.Status status;

  public TopInstanceCommand(String projectName, String commandText, ExecutionContext context) {
    super(commandText, context);

    this.projectName = projectName;
  }

  public void setNubmer(Integer nubmer) {
    if (nubmer != null) {
      this.nubmer = nubmer;
    }
  }

  public void setDelay(Integer delay) {
    if (delay != null && delay > 0) {
      this.delay = delay;
    }
  }

  public void setOnlyOwner(Boolean onlyOwner) {
    if (onlyOwner != null) {
      this.onlyOwner = onlyOwner;
    }
  }

  public void setStatus(Instance.Status status) {
    if (status != null) {
      this.status = status;
    }
  }

  static Options initOptions() {
    Options opts = new Options();

    Option project = new Option(PROJECT_TAG, true, "project name");
    project.setRequired(false);
    opts.addOption(project);

    Option limit = new Option(LIMIT_TAG, true, "limit number");
    limit.setRequired(false);
    opts.addOption(limit);

    Option status = new Option(STATUS_TAG, true, "instance status");
    status.setRequired(false);
    opts.addOption(status);

    Option all = new Option(ALL_TAG, false, "all, only owner");
    all.setRequired(false);
    opts.addOption(all);

    Option delay = new Option(DELAY_TAG, true, "delay seconds");
    delay.setRequired(false);
    delay.setOptionalArg(true);
    opts.addOption(delay);

    return opts;
  }

  static CommandLine getCommandLine(String cmd) throws ODPSConsoleException {

    AntlrObject antlr = new AntlrObject(cmd);
    String[] args = antlr.getTokenStringArray();

    if (args == null) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  private String prepareFooter(int total) {
    return String.format("QueueingInstances: %d total.", total);
  }

  private String getQueueingMessage(Instance.InstanceQueueingInfo info) {

    Long cpu = info.getProperty("cpuUsage", Long.class);
    Long mem = info.getProperty("memUsage", Long.class);

    String runtimeUsage = "";
    if (cpu != null && mem != null) {
      runtimeUsage = String.format("%d/%d", cpu, mem);
    }

    cpu = info.getProperty("totalCpuUsage", Long.class);
    mem = info.getProperty("totalMemUsage", Long.class);

    String totalUsage = "";
    if (cpu != null && mem != null) {
      totalUsage = String.format("%d/%d", cpu, mem);
    }

    String queuePos = "";
    Long pos = info.getProperty("waitPos", Long.class);
    Long length = info.getProperty("queueLength", Long.class);

    if (pos != null && length != null) {
      queuePos = String.format("%d/%d", pos, length);
    }

    String status = info.getStatus().toString();
    JsonObject subStatus = info.getProperty("subStatus", JsonObject.class);
    if (subStatus != null && !StringUtils.isNullOrEmpty(subStatus.get("description").getAsString())) {
      status = String.format("%s(%s)", status, subStatus.get("description").getAsString());
    }

    String type = info.getTaskType() == null ? "" : info.getTaskType();

    Double progress = info.getProgress();
    String progressStr = "";
    if (progress != null && progress >= 0) {
      progressStr = String.format("%.2f%%", progress * 100);
    }

    Date startTime = info.getStartTime();

    String timeStr = "";
    if (startTime != null) {
      timeStr = DATE_FORMAT.format(startTime);
    }

    return String
        .format(HEADER_FORMAT, info.getId(), info.getUserAccount(), type, timeStr,
                progressStr, status, info.getPriority(), runtimeUsage, totalUsage, queuePos);
  }

  private Iterator<Instance.InstanceQueueingInfo> getQueue(Odps odps) throws OdpsException {
    InstanceFilter filter = new InstanceFilter();
    filter.setOnlyOwner(onlyOwner);
    filter.setStatus(status);

    if (projectName == null) {
      projectName = odps.getDefaultProject();
    }

    Iterator<Instance.InstanceQueueingInfo> iter = odps.instances().iteratorQueueing(projectName, filter);

    iter.hasNext(); // check permission

    return iter;
  }

  private void writeHeader() {
    if (InPlaceUpdates.isUnixTerminal()) {
      InPlaceUpdates.reprintLineWithColorAsBold(ERR, HEADER, Ansi.Color.CYAN);
    } else {
      getContext().getOutputWriter().writeError(HEADER);
    }
  }

  private int printQueue(Iterator<Instance.InstanceQueueingInfo> queue) throws OdpsException {

    int total = 0;
    if (queue.hasNext()) {
      writeHeader(); // a little tricky
      getContext().getOutputWriter().writeError(SEPARATOR);

      while (queue.hasNext() && total++ <= nubmer) {
        getContext().getOutputWriter().writeError(getQueueingMessage(queue.next()));
      }
      getContext().getOutputWriter().writeError(SEPARATOR);
    } else {
      getContext().getOutputWriter().writeError(prepareFooter(0));
    }

    return total;
  }

  private void autoRefreshQueue(Odps odps) throws OdpsException {
    boolean first = true;
    while (true) {
      Iterator<Instance.InstanceQueueingInfo> queue = getQueue(odps);

      if (first && !queue.hasNext()) {
        getContext().getOutputWriter().writeError(prepareFooter(0));
        return;
      }

      if (first) {
        InPlaceUpdates.clearScreen(ERR);
        first = false;
      } else {
        InPlaceUpdates.resetScreen(ERR);
      }

      if (0 == printQueue(queue)) {
        break;
      }

      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(delay));
      } catch (InterruptedException e) {
        throw new UserInterruptException("User interrupted.");
      }
    }
  }

  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    if ((delay != null) && InPlaceUpdates.isUnixTerminal()) {
      autoRefreshQueue(odps);
    } else {
      printQueue(getQueue(odps));
    }
  }

  private static final Pattern PATTERN =
      Pattern.compile("TOP\\s+INSTANCE(\\s+([\\s\\S]*)\\s*|\\s*)", Pattern.CASE_INSENSITIVE);

  public static TopInstanceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    Matcher matcher = PATTERN.matcher(commandString);
    String projectName = null;
    Integer delay = null;
    Boolean onlyOwner = null;
    Integer number = null;
    Instance.Status status = null;

    if (matcher.matches()) {
      if (!StringUtils.isNullOrEmpty(matcher.group(2))) {
        CommandLine commandLine = getCommandLine(matcher.group(2));

        if (!commandLine.getArgList().isEmpty()) {
          throw new ODPSConsoleException(
              ODPSConsoleConstants.BAD_COMMAND + "[too much parameters.]");
        }

        if (commandLine.hasOption(PROJECT_TAG)) {
          projectName = commandLine.getOptionValue(PROJECT_TAG);
        }

        if (commandLine.hasOption(STATUS_TAG)) {
          try {
            status = Instance.Status.valueOf(commandLine.getOptionValue(STATUS_TAG).toUpperCase());
          } catch (IllegalArgumentException e) {
            throw new ODPSConsoleException(
                ODPSConsoleConstants.BAD_COMMAND + "[bad status value.]");
          }
        }

        if (commandLine.hasOption(LIMIT_TAG)) {
          String val = commandLine.getOptionValue(LIMIT_TAG);
          if (org.apache.commons.lang.StringUtils.isNumeric(val)) {
            number = Integer.parseInt(val);

            if (number <= 0) {
              throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                             + "[bad limit value, limit number should bigger than zero.]");
            }
          } else {
            throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[bad limit value.]");
          }
        }

        if (commandLine.hasOption(DELAY_TAG)) {
          String val = commandLine.getOptionValue(DELAY_TAG);
          if (StringUtils.isNullOrEmpty(val)) {
            delay = DEFAULT_DELAY;
          } else if (org.apache.commons.lang.StringUtils.isNumeric(val)
                     && Integer.parseInt(val) > 0) {
            delay = Integer.parseInt(val);
          } else {
            throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[bad delay value.]");
          }
        }

        if (commandLine.hasOption(ALL_TAG)) {
          onlyOwner = false;
        }
      }

      TopInstanceCommand
          command =
          new TopInstanceCommand(projectName, commandString, sessionContext);
      command.setOnlyOwner(onlyOwner);
      command.setStatus(status);
      command.setDelay(delay);
      command.setNubmer(number);

      return command;
    }

    return null;
  }
}

package com.aliyun.openservices.odps.console.commands;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExternalProjectCommand extends AbstractCommand {
  private static final String NAME_OPTION = "name";
  private static final String COMMENT_OPTION = "comment";
  private static final String REF_OPTION = "ref";
  private static final String NAMENODE_OPTION = "nn";
  private static final String HMS_OPTION = "hms";
  private static final String DATABASE_OPTION = "db";
  private static final String VPC_OPTION = "vpc";
  private static final String REGION_OPTION = "region";
  private static final String ACCESS_IP_OPTION = "accessIp";
  private static final String DFS_NS_OPTION = "dfsNamespace";

  private final static String CREATE_ACTION = "create";
  private final static String UPDATE_ACTION = "update";
  private final static String DELETE_ACTION = "delete";

  private final static Pattern PATTERN = Pattern.compile(
          "\\s*(" + CREATE_ACTION + "|" + UPDATE_ACTION + "|" + DELETE_ACTION +")\\s+EXTERNALPROJECT\\s+(.+)",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private Action action;

  private ExternalProjectCommand(Action action, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.action = action;
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {
    action.run(getCurrentOdps());
  }

  public Action getAction() {
    return action;
  }

  public static final String[] HELP_TAGS = new String[]{"create", "externalproject", "external", "project"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: create externalproject -name <project name> -ref <referred managed project>  [-comment <comment>]");
    stream.println("                              -nn <namenode ips> -hms <hive metastore ips> -db <hive database name>");
    stream.println("                              [-vpc <vpc id> -region <vpc region> -accessIp <vpc internal ips>] [-dfsNamespace <hive ha dfs namespace>]");

    stream.println("Usage: update externalproject -name <project name>");
    stream.println("                              -nn <namenode ips> -hms <hive metastore ips> -db <hive database name>");
    stream.println("                              [-vpc <vpc id> -region <vpc region> -accessIp <vpc internal ips>] [-dfsNamespace <hive ha dfs namespace>]");

    stream.println("Usage: delete externalproject -name <project name>");
    stream.println("");
    stream.println("All ip addresses require ports specified: 'ip:port'. Multiple ip addresses should be delimited by comma.");
    stream.println("Examples:");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838\" -db default;");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838\" -db default ");
    stream.println("                         -vpc vpc1 -region cn-zhangjiakou -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"");
    stream.println("                         -dfsNamespace emr-cluster;");
  }

  public static AbstractCommand parse(String cmd, ExecutionContext sessionContext)
          throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String actionName = m.group(1);
    String input = m.group(2);
    String[] inputs = ODPSConsoleUtils.translateCommandline(input);

    Options options = new Options();
    options.addOption(Option.builder(ExternalProjectCommand.NAME_OPTION)
            .hasArg().required().desc("Project name.").build());
    options.addOption(COMMENT_OPTION, true, "Project description.");
    options.addOption(REF_OPTION, true,"Managed Project refs to.");
    options.addOption(NAMENODE_OPTION, true, "Hadoop namenode ip and ports.");
    options.addOption(HMS_OPTION, true,"Hive metastore ip and ports.");
    options.addOption(DATABASE_OPTION, true, "Hive database name.");
    options.addOption(VPC_OPTION, true, "Vpc id");
    options.addOption(REGION_OPTION, true, "Vpc region.");
    options.addOption(ACCESS_IP_OPTION, true, "Additional vpc ip need to be accessed");
    options.addOption(DFS_NS_OPTION, true, "Hive DFS nameservice.");
    try {
      CommandLineParser parser = new DefaultParser();
      Action action = new Action(actionName, parser.parse(options, inputs, false));
      return new ExternalProjectCommand(action, cmd, sessionContext);
    } catch (ParseException e) {
      throw new ODPSConsoleException("Error parsing command", e);
    }
  }

  public static class Action {
    private String actionName;

    private String projectName;
    private String comment;
    private String refProjectName;
    private String nameNodes;
    private String hiveMetastores;
    private String databaseName;
    private String vpcId;
    private String vpcRegion;
    private String accessIp;
    private String dfsNamespace;

    private final static Set<String> validActions = new HashSet<>(Arrays.asList(CREATE_ACTION, UPDATE_ACTION, DELETE_ACTION));

    Action(String action, CommandLine params)  {
      this.actionName = action.toLowerCase();
      this.projectName = params.getOptionValue(NAME_OPTION);
      this.comment = params.getOptionValue(COMMENT_OPTION);
      this.refProjectName = params.getOptionValue(REF_OPTION);
      this.nameNodes = params.getOptionValue(NAMENODE_OPTION);
      this.hiveMetastores = params.getOptionValue(HMS_OPTION);
      this.databaseName = params.getOptionValue(DATABASE_OPTION);
      this.vpcId = params.getOptionValue(VPC_OPTION);
      this.vpcRegion = params.getOptionValue(REGION_OPTION);
      this.accessIp = params.getOptionValue(ACCESS_IP_OPTION);
      this.dfsNamespace = params.getOptionValue(DFS_NS_OPTION);

      validateParams();
    }

    public void run(Odps odps) throws OdpsException {
      switch (actionName) {
        case CREATE_ACTION: {
          odps.projects().createExternalProject(projectName, comment, refProjectName, buildExternalProjectProperties());
          break;
        }
        case UPDATE_ACTION: {
          Project.ExternalProjectProperties extProps = buildExternalProjectProperties();
          HashMap<String, String> props = new HashMap<>();
          props.put("external_project_properties", extProps.toJson());
          odps.projects().updateProject(projectName, props);
          break;
        }
        case DELETE_ACTION: {
          odps.projects().deleteExternalProject(projectName);
          break;
        }
      }
    }

    public String getActionName() {
      return actionName;
    }

    private Project.ExternalProjectProperties buildExternalProjectProperties() {
      Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("hive");
      extProps.addProperty("hms.ips", hiveMetastores);
      extProps.addProperty("hive.database.name", databaseName);
      extProps.addProperty("hdfs.namenode.ips", nameNodes);

      if (vpcId != null) {
        extProps.addNetworkProperty("odps.external.net.vpc", "true");
        extProps.addNetworkProperty("odps.vpc.id", vpcId);
        extProps.addNetworkProperty("odps.vpc.region", vpcRegion);
        extProps.addNetworkProperty("odps.vpc.access.ips", accessIp);
      } else {
        extProps.addNetworkProperty("odps.external.net.vpc", "false");
      }

      if (dfsNamespace != null) {
        extProps.addNetworkProperty("dfs.nameservices", dfsNamespace);
      }

      return extProps;
    }

    private void validateParams()  {
      if (!validActions.contains(actionName)) {
        throw new IllegalArgumentException("Unknown action: " + actionName);
      }

      require(NAME_OPTION, projectName);

      if (actionName.equals(DELETE_ACTION)) {
        return;
      }

      require(NAMENODE_OPTION, nameNodes);
      require(HMS_OPTION, hiveMetastores);
      require(DATABASE_OPTION, databaseName);

      if (vpcId != null) {
        require(VPC_OPTION, vpcId);
        require(REGION_OPTION, vpcRegion);
        require(ACCESS_IP_OPTION, accessIp);
      }

      if (actionName.equals(CREATE_ACTION)) {
        require(REF_OPTION, refProjectName);
      }
    }

    private static void require(String name, String value) {
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException(name + " is required and not allowed to be empty.");
      }
    }
  }
}

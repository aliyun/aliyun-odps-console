package com.aliyun.openservices.odps.console.commands;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import com.google.gson.JsonObject;
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
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExternalProjectCommand extends AbstractCommand {
  private static final String SOURCE_OPTION = "source";
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
  private static final String PROPERTIES_OPTION = "D";
  private static final String ROLE_ARN_OPTION = "ramRoleArn";
  private static final String ENDPOINT_OPTION = "endpoint";
  private static final String OSS_ENDPOINT_OPTION = "ossEndpoint";
  private static final String TABLE_PROPERTIES_OPTION = "T";
  private static final String HMS_PRINCIPALS = "hmsPrincipals";
  private static final String FOREIGNSERVER_OPTION = "foreignServer";

  private final static String CREATE_ACTION = "create";
  private final static String UPDATE_ACTION = "update";
  private final static String DELETE_ACTION = "delete";

  private final static Pattern PATTERN = Pattern.compile(
          "\\s*(" + CREATE_ACTION + "|" + UPDATE_ACTION + "|" + DELETE_ACTION +")\\s+EXTERNALPROJECT\\s+(.+)",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final Action action;

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
    stream.println("                              [-nn <namenode ips> -hms <hive metastore ips>]|[-foreignServer <server_name>] -db <hive database name> [-hmsPrincipals <kerberos principals of hms>]");
    stream.println("                              [-vpc <vpc id> -region <vpc region> [-accessIp <vpc internal ips>]] [-dfsNamespace <hive ha dfs namespace>]");
    stream.println("                              [-D <property name 1>=<value1> ...];");

    stream.println("Usage: create externalproject -source dlf -name <project name> -ref <referred managed project>  [-comment <comment>]");
    stream.println("                              -region <dlf region> -db <dlf database name> -endpoint <dlf endpoint> [-ramRoleArn <ram role arn to access dlf>]");
    stream.println("                              [-D <property name 1>=<value1> ...];");

    stream.println("Usage: update externalproject -name <project name>");
    stream.println("                              -nn <namenode ips> -hms <hive metastore ips> -db <hive database name> [-hmsPrincipals <kerberos principals of hms>]");
    stream.println("                              [-vpc <vpc id> -region <vpc region> [-accessIp <vpc internal ips>]] [-dfsNamespace <hive ha dfs namespace>]");
    stream.println("                              [-D <property name 1>=<value1> ...];");

    stream.println("Usage: update externalproject -source dlf -name <project name>");
    stream.println("                              -region <dlf region> -db <dlf database name> -endpoint <dlf endpoint> [-ramRoleArn <ram role arn to access dlf>]");
    stream.println("                              [-ossEndpoint <oss endpoint>]");
    stream.println("                              [-T <table property name 1>=<value1> ...] [-D <property name 1>=<value1> ...];");

    stream.println("Usage: delete externalproject -name <project name>");
    stream.println();
    stream.println("1. All ip addresses require ports specified: 'ip:port'. Multiple ip addresses should be delimited by comma.");
    stream.println("2. To see all supported table properties you can set via '-T p=v', refer to external project documentation.");
    stream.println("3. To see how to specify 'hmsPrincipals' and 'dfs.data.transfer.protection' for kerberos hive, refer to external project documentation.");
    stream.println();
    stream.println("## Examples:");
    stream.println("#### hive db:");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838\" -db default;");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838\" -db default ");
    stream.println("                         -vpc vpc1 -region cn-zhangjiakou -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"");
    stream.println("                         -dfsNamespace emr-cluster;");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838\" -db default ");
    stream.println("                         -vpc vpc1 -region cn-zhangjiakou -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"");
    stream.println("                         -dfsNamespace emr-cluster -D odps.properties.rolearn=samplerolearn -D another.property=abcde;");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -foreignServer server_1 -db default;");
    stream.println("#### hive db with kerberos:");
    stream.println("  create externalproject -name p1 -ref myprj1 -comment test -nn \"1.1.1.1:8080,2.2.2.2:3823\" -hms \"3.3.3.3:3838,4.4.4.4:3838\" -db default ");
    stream.println("                         -hmsPrincipals \"hive/emr-header-1.cluster-203713@EMR.203713.COM,hive/emr-header-2.cluster-203713@EMR.203713.COM\"");
    stream.println("                         -vpc vpc1 -region cn-zhangjiakou -accessIp \"192.168.0.11-192.168.0.14:50010,192.168.0.23:50020\"");
    stream.println("                         -dfsNamespace emr-cluster -D dfs.data.transfer.protection=integrity;");
    stream.println("#### dlf:");
    stream.println("  create externalproject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default ");
    stream.println("                         -endpoint \"dlf.cn-shanghai.aliyuncs.com\";");
    stream.println("  create externalproject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default ");
    stream.println("                         -endpoint \"dlf.cn-shanghai.aliyuncs.com\" -ramRoleArn \"acs:ram::12345:role/myrolefordlfonodps\";");
    stream.println("  create externalproject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default ");
    stream.println("                         -endpoint \"dlf.cn-shanghai.aliyuncs.com\" -D a.property=ddfjie -D another.property=abcde;");
    stream.println("  create externalproject -source dlf -name p1 -ref myprj1 -comment test -region cn-shanghai -db default ");
    stream.println("                         -endpoint \"dlf.cn-shanghai.aliyuncs.com\" -ossEndpoint \"oss-cn-shanghai-internal.aliyuncs.com\" ");
    stream.println("                         -T file_format=orc -T output_format=text -D another.property=abcde;");
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
    options.addOption(SOURCE_OPTION, true, "External project source - supported sources: hive,dlf. Default: hive.");
    options.addOption(COMMENT_OPTION, true, "Project description.");
    options.addOption(REF_OPTION, true,"Managed Project refs to.");
    options.addOption(FOREIGNSERVER_OPTION, true,"Using foreign server name.");
    options.addOption(NAMENODE_OPTION, true, "Hadoop namenode ip and ports.");
    options.addOption(HMS_OPTION, true,"Hive metastore ip and ports.");
    options.addOption(DATABASE_OPTION, true, "Database name to map external project to.");
    options.addOption(VPC_OPTION, true, "Vpc id");
    options.addOption(REGION_OPTION, true, "Region.");
    options.addOption(ACCESS_IP_OPTION, true, "Additional vpc ip need to be accessed");
    options.addOption(DFS_NS_OPTION, true, "Hive DFS nameservice.");
    options.addOption(ROLE_ARN_OPTION, true, "Ram role arn.");
    options.addOption(ENDPOINT_OPTION, true, "Endpoint of external source.");
    options.addOption(OSS_ENDPOINT_OPTION, true, "Endpoint of oss - for dlf source which use oss as storage.");
    options.addOption(HMS_PRINCIPALS, true, "Comma separated kerberos principals corresponding to host specified in 'hms'.");


    Option pOption = new Option(PROPERTIES_OPTION, true, "Additional parameters, like '-D p1=v1 -D p2=v2.");
    pOption.setValueSeparator('=');
    pOption.setArgs(2);
    options.addOption(pOption);

    Option tOption = new Option(TABLE_PROPERTIES_OPTION, true, "Additional table parameters, like '-T p1=v1 -T p2=v2.");
    tOption.setValueSeparator('=');
    tOption.setArgs(2);
    options.addOption(tOption);
    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine params = parser.parse(options, inputs, false);
      // FIXME: determine source type by foreign server if it's there
      String source = params.getOptionValue(SOURCE_OPTION, "hive");
      Action action;
      if (source.equals("hive")) {
        action = new HiveSourceAction(actionName, params);
      } else if(source.equals("dlf")) {
        action = new DlfSourceAction(actionName, params);
      } else if(source.equals("maxcompute")) {
        action = new OdpsSourceAction(actionName, params);
      } else {
        throw new UnsupportedOperationException("Unknown source: " + source);
      }
      return new ExternalProjectCommand(action, cmd, sessionContext);
    } catch (ParseException e) {
      throw new ODPSConsoleException("Error parsing command", e);
    }
  }

  public static abstract class Action {
    private final String actionName;
    private final String projectName;
    private final String comment;
    private String refProjectName;
    private Properties properties = new Properties();
    private final static Set<String> validActions = new HashSet<>(Arrays.asList(CREATE_ACTION, UPDATE_ACTION, DELETE_ACTION));

    Action(String action, CommandLine params) {
      this.actionName = action.toLowerCase();
      this.projectName = params.getOptionValue(NAME_OPTION);
      this.comment = params.getOptionValue(COMMENT_OPTION);
      this.refProjectName = params.getOptionValue(REF_OPTION);
      if (params.hasOption(PROPERTIES_OPTION)) {
        properties = params.getOptionProperties(PROPERTIES_OPTION);
      }
      validateParams();
    }

    protected abstract Project.ExternalProjectProperties buildExternalProjectProperties();

    public Project.ExternalProjectProperties finalExternalProjectProperties() {
      Project.ExternalProjectProperties extProps = buildExternalProjectProperties();
      for(String name : properties.stringPropertyNames()) {
        String value = properties.getProperty(name).replaceAll("^[\"']|[\"']$", "");
        extProps.addNetworkProperty(name,value);
      }

      return extProps;
    }

    void run(Odps odps) throws OdpsException {
      switch (actionName) {
        case CREATE_ACTION: {
          if (StringUtils.isNullOrEmpty(refProjectName)) {
            refProjectName = odps.getDefaultProject();
          }

          odps.projects().createExternalProject(
                  projectName, comment, refProjectName, finalExternalProjectProperties());
          break;
        }
        case UPDATE_ACTION: {
          Project.ExternalProjectProperties extProps = finalExternalProjectProperties();
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

    public final String getActionName() {
      return actionName;
    }

    private void validateParams()  {
      if (!validActions.contains(actionName)) {
        throw new IllegalArgumentException("Unknown action: " + actionName);
      }

      require(NAME_OPTION, projectName);

      if (actionName.equals(DELETE_ACTION)) {
        return;
      }
    }

    protected static void require(String name, String value) {
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException(name + " is required and not allowed to be empty.");
      }
    }
  }
  public static class HiveSourceAction extends Action {
    private final String nameNodes;
    private final String hiveMetastores;
    private final String databaseName;
    private final String vpcId;
    private final String vpcRegion;
    private final String accessIp;
    private final String dfsNamespace;
    private final String hmsPrincipals;
    private final String serverName;

    HiveSourceAction(String action, CommandLine params)  {
      super(action, params);
      this.nameNodes = params.getOptionValue(NAMENODE_OPTION);
      this.hiveMetastores = params.getOptionValue(HMS_OPTION);
      this.databaseName = params.getOptionValue(DATABASE_OPTION);
      this.vpcId = params.getOptionValue(VPC_OPTION);
      this.vpcRegion = params.getOptionValue(REGION_OPTION);
      this.accessIp = params.hasOption(ACCESS_IP_OPTION) ?
              params.getOptionValue(ACCESS_IP_OPTION) : params.getOptionValue(NAMENODE_OPTION);
      this.dfsNamespace = params.getOptionValue(DFS_NS_OPTION);
      this.hmsPrincipals = params.getOptionValue(HMS_PRINCIPALS);
      this.serverName = params.getOptionValue(FOREIGNSERVER_OPTION);

      validateParams();
    }

    @Override
    protected Project.ExternalProjectProperties buildExternalProjectProperties() {
      Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("hive");
      extProps.addProperty("hive.database.name", databaseName);

      if (serverName != null) {
        extProps.addProperty("foreign.server.name", serverName);
      } else {
        extProps.addProperty("hms.ips", hiveMetastores);
        extProps.addProperty("hdfs.namenode.ips", nameNodes);
      }

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

      if (hmsPrincipals != null) {
        extProps.addProperty("hms.principals", hmsPrincipals);
      }

      return extProps;
    }

    private void validateParams()  {
      if (getActionName().equals(DELETE_ACTION)) {
        return;
      }

      require(DATABASE_OPTION, databaseName);

      if (serverName != null)
      {
        return;
      }

      require(NAMENODE_OPTION, nameNodes);
      require(HMS_OPTION, hiveMetastores);

      if (vpcId != null) {
        require(VPC_OPTION, vpcId);
        require(REGION_OPTION, vpcRegion);
        require(ACCESS_IP_OPTION, accessIp);
      }
    }
  }

  public static class DlfSourceAction extends Action {
    private final String region;
    private final String endpoint;
    private final String databaseName;
    private final String roleArn;
    private final String ossEndpoint;
    private Properties tableProperties = new Properties();

    DlfSourceAction(String action, CommandLine params)  {
      super(action, params);

      this.region = params.getOptionValue(REGION_OPTION);
      this.endpoint = params.getOptionValue(ENDPOINT_OPTION);
      this.databaseName = params.getOptionValue(DATABASE_OPTION);
      this.roleArn = params.getOptionValue(ROLE_ARN_OPTION);
      this.ossEndpoint = params.getOptionValue(OSS_ENDPOINT_OPTION);
      if (params.hasOption(TABLE_PROPERTIES_OPTION)) {
        tableProperties = params.getOptionProperties(TABLE_PROPERTIES_OPTION);
      }
      validateParams();
    }

    @Override
    protected Project.ExternalProjectProperties buildExternalProjectProperties() {
      Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("dlf");
      extProps.addProperty("dlf.region", region );
      extProps.addProperty("dlf.endpoint", endpoint);
      extProps.addProperty("dlf.database.name", databaseName);
      extProps.addProperty("dlf.rolearn", roleArn);

      if (ossEndpoint != null) {
        extProps.addProperty("oss.endpoint", ossEndpoint);
      }

      if (!tableProperties.isEmpty()) {
        JsonObject obj = new JsonObject();
        for (String name : tableProperties.stringPropertyNames()) {
          String value = tableProperties.getProperty(name).replaceAll("^[\"']|[\"']$", "");
          obj.addProperty(name, value);
        }
        extProps.addProperty("table_properties", obj);
      }

      return extProps;
    }

    private void validateParams()  {
      if (getActionName().equals(DELETE_ACTION)) {
        return;
      }

      require(REGION_OPTION, region);
      require(ENDPOINT_OPTION, endpoint);
      require(DATABASE_OPTION, databaseName);
    }
  }

  public static class OdpsSourceAction extends Action {

    private final String serverName;
    private final String databaseName;

    OdpsSourceAction(String action, CommandLine params)  {
      super(action, params);
      this.serverName = params.getOptionValue(FOREIGNSERVER_OPTION);
      this.databaseName = params.getOptionValue(DATABASE_OPTION);
      validateParams();
    }

    @Override
    protected Project.ExternalProjectProperties buildExternalProjectProperties() {
      Project.ExternalProjectProperties extProps = new Project.ExternalProjectProperties("maxcompute");
      extProps.addProperty("maxcompute.database.name", databaseName);
      extProps.addNetworkProperty("odps.external.net.vpc", "false");

      if (serverName != null) {
        extProps.addProperty("foreign.server.name", serverName);
      }

      return extProps;
    }

    private void validateParams()  {
      if (getActionName().equals(DELETE_ACTION)) {
        return;
      }
      require(FOREIGNSERVER_OPTION, serverName);
      require(DATABASE_OPTION, databaseName);
    }
  }
}

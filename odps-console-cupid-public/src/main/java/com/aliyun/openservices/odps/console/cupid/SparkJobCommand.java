/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console.cupid;

import apsara.odps.cupid.protocol.CupidTaskParamProtos;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.cupid.common.CupidConstants;
import com.aliyun.openservices.odps.console.cupid.common.CupidSessionConf;
import com.aliyun.openservices.odps.console.cupid.common.SparkJobUtils;
import com.aliyun.openservices.odps.console.cupid.common.YarnConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SparkJobCommand extends AbstractCommand
{
    public static final String[] HELP_TAGS = new String[]{"spark"};

    private static final String LIST_CMD = "list";
    private static final String INFO_CMD = "info";
    private static final String KILL_CMD = "kill";
    private static final String SEARCH_CMD = "search";
    private static final String VIEW_CMD = "view";

    private static final String INSTANCE_ID_ARG = "instanceId";
    private static final String APPLICATION_ID_ARG = "appId";
    private static final String STATE_ARG = "state";

    private String subCommand;
    private String instanceId;
    private String applicationId;
    private String applicationName;
    private String stateFilter = null;
    private CupidConf cupidConf;

    String getInstanceId() {
        return instanceId;
    }

    String getApplicationId() {
        return applicationId;
    }

    String getSubCommand() {
        return subCommand;
    }

    String getApplicationName() {
        return applicationName;
    }

    public static void printUsage(PrintStream stream) {
        stream.println("Usage: spark list [-s <yarnState>(NEW,RUNNING,FINISHED,FAILED,KILLED)];");
        stream.println("       spark info [-i <instanceId>] [-a <appId>];");
        stream.println("       spark kill [-i <instanceId>] [-a <appId>];");
        stream.println("       spark view [-i <instanceId>] [-a <appId>];");
        stream.println("       spark search <appNameStr>;");
    }

    public SparkJobCommand(String commandText, ExecutionContext context, String subCommand,
                           String[] args) throws ODPSConsoleException {
        super(commandText, context);
        this.subCommand = subCommand;

        try {
            parseArgs(args);
        } catch (ParseException e) {
            throw new ODPSConsoleException(e.getMessage());
        }
    }

    public static SparkJobCommand parse(String cmd, ExecutionContext cxt)
            throws ODPSConsoleException{
        String regStr = "\\s*spark\\s+(list|info|kill|search|view)([\\s\\S]*)";
        Pattern pattern = Pattern.compile(regStr, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(cmd);
        if (matcher.matches()) {
            String args = matcher.group(2).replaceAll("\\s+", " ").trim();
            return new SparkJobCommand(cmd, cxt, matcher.group(1), args.split(" "));
        }

        return null;
    }

    private static Options initListOptions() {
        Options options = new Options();

        Option stateOption = new Option("s", STATE_ARG, true,
            "optional, yarn application state filter");
        stateOption.setRequired(false);
        options.addOption(stateOption);

        return options;
    }

    private static Options initSearchOptions() {
        return new Options();
    }

    private static Options initKillOptions() {
        return initCommonOptions();
    }

    private static Options initInfoOptions() {
        return initCommonOptions();
    }

    private static Options initViewOptions() {
        return initCommonOptions();
    }

    private static Options initCommonOptions() {
        Options options = new Options();

        Option instanceIdOption = new Option("i", INSTANCE_ID_ARG, true,
                "optional, instance id");
        instanceIdOption.setRequired(false);
        options.addOption(instanceIdOption);

        Option applicationIdOption = new Option("a", APPLICATION_ID_ARG, true,
                "optional, application id");
        applicationIdOption.setRequired(false);
        options.addOption(applicationIdOption);

        return options;
    }

    private void parseArgs(String[] args)
            throws ParseException, ODPSConsoleException {
        CommandLineParser parser = new DefaultParser();

        // Choose options
        Options options;
        if (LIST_CMD.equalsIgnoreCase(this.subCommand)) {
            options = initListOptions();
        }
        else if (INFO_CMD.equalsIgnoreCase(this.subCommand)) {
            options = initInfoOptions();
        }
        else if (KILL_CMD.equalsIgnoreCase(this.subCommand)) {
            options = initKillOptions();
        }
        else if (SEARCH_CMD.equalsIgnoreCase(this.subCommand)) {
            this.applicationName = StringUtils.join(args, " ");
            options = initSearchOptions();
        }
        else if (VIEW_CMD.equalsIgnoreCase(this.subCommand)) {
            options = initViewOptions();
        }
        else {
            printUsage(System.err);
            throw new ODPSConsoleException("Invalid command \"" + this.subCommand + "\". ");
        }

        CommandLine cmd = parser.parse(options, args);

        // Set Command instanceId or applicationId by command text
        if (INFO_CMD.equalsIgnoreCase(subCommand) ||
            KILL_CMD.equalsIgnoreCase(subCommand) ||
            VIEW_CMD.equalsIgnoreCase(subCommand)) {
            setInstanceIdOrAppId(cmd);
        } else if (LIST_CMD.equalsIgnoreCase(subCommand)) {
            if (cmd.hasOption(STATE_ARG)) {
                // comma split state list to filter
                // NEW,RUNNING,FINISHED,FAILED,KILLED
                String statesString = cmd.getOptionValue(STATE_ARG);
                String[] states = statesString.split(",");
                this.stateFilter = Arrays.stream(states)
                    .map(f -> String.valueOf(YarnConstants.getAppStateCode(f)))
                    .collect(Collectors.joining(","));
            }
        }

        // initialize cupidConf
        this.cupidConf = CupidSessionConf.getBasicCupidConf(this.getContext());
    }

    private void setInstanceIdOrAppId(CommandLine cmd) throws ODPSConsoleException {
        if (cmd.hasOption(INSTANCE_ID_ARG)) {
            this.instanceId = cmd.getOptionValue(INSTANCE_ID_ARG);
        }
        else if (cmd.hasOption(APPLICATION_ID_ARG)) {
            this.applicationId = cmd.getOptionValue(APPLICATION_ID_ARG);
        }
        else {
            throw new ODPSConsoleException("Need to specify at least one of the instanceId and applicationId");
        }
    }

    @Override
    public void run() throws ODPSConsoleException, OdpsException {
        if (LIST_CMD.equalsIgnoreCase(subCommand)) {
            listSparkJobs();
        }
        else if (INFO_CMD.equalsIgnoreCase(subCommand)) {
            sparkJobInfo();
        }
        else if (KILL_CMD.equalsIgnoreCase(subCommand)) {
            killSparkJob();
        }
        else if (SEARCH_CMD.equalsIgnoreCase(subCommand)) {
            searchSparkJob();
        }
        else if (VIEW_CMD.equalsIgnoreCase(subCommand)) {
            generateSparkViews();
        }
    }

    /**
     * get ApplicationMetaList from cupid, and format print into console
     * @throws ODPSConsoleException
     */
    private void listSparkJobs() throws ODPSConsoleException {
        CupidSession session = new CupidSession(cupidConf);

        CupidTaskParamProtos.ApplicationMetaList applicationMetaList =
            SparkJobUtils.getApplicationMetaList(session, this.stateFilter);
        String[] title = {"StartTime", "InstanceId", "State", "RunningMode", "ApplicationName"};
        // 设置每一列的百分比
        int[] columnPercent = {20, 20, 10, 10, 40};
        ODPSConsoleUtils.formaterTableRow(title, columnPercent, getContext().getConsoleWidth());
        String[] attr = new String[5];
        for (CupidTaskParamProtos.ApplicationMeta applicationMeta : applicationMetaList.getApplicationMetaListList()) {
            long sTime = applicationMeta.getStartedTime();
            Date date = new Date(sTime);
            attr[0] = ODPSConsoleUtils.formatDate(date);
            attr[1] = applicationMeta.getInstanceId();

            long intState = applicationMeta.getYarnApplicationState();
            attr[2] = YarnConstants.getAppStateStr(intState);
            attr[3] = applicationMeta.getRunningMode();
            attr[4] = applicationMeta.getApplicationName();
            ODPSConsoleUtils.formaterTableRow(attr, columnPercent, getContext().getConsoleWidth());
        }
    }

    /**
     * get single spark job from cupid via instanceId or applicationId
     * format print into odps console
     * @throws ODPSConsoleException
     */
    private void sparkJobInfo() throws ODPSConsoleException {
        CupidSession session = new CupidSession(cupidConf);
        CupidTaskParamProtos.ApplicationMeta applicationMeta =
            SparkJobUtils.getApplicationMeta(instanceId, applicationId, session);
        getWriter().writeError(applicationMeta.toString());
    }

    /**
     * kill spark job via instanceId or applicationId
     * @throws ODPSConsoleException
     * @throws OdpsException
     */
    private void killSparkJob() throws ODPSConsoleException, OdpsException {
        CupidSession session = new CupidSession(cupidConf);

        CupidTaskParamProtos.ApplicationMeta applicationMeta =
            SparkJobUtils.getApplicationMeta(instanceId, applicationId, session);

        // stop spark job via odps sdk
        try {
            getCurrentOdps().instances()
                .get(applicationMeta.getInstanceId())
                .stop();

            getWriter().writeError("please check instance status. [status " + applicationMeta.getInstanceId() + ";]");
        } catch (Exception ex) {
            getWriter().writeError("stop instance failed. " + ex.getMessage());
        }

        // try update meta if needed
        if (applicationMeta.getFinalApplicationStatus() == YarnConstants.FinalApplicationStatus.UNDEFINED.ordinal()) {
            CupidTaskParamProtos.ApplicationMeta.Builder updateMeta = CupidTaskParamProtos.ApplicationMeta.newBuilder();
            updateMeta.setYarnApplicationState(YarnConstants.AppState.KILLED.ordinal());
            updateMeta.setFinalApplicationStatus(YarnConstants.FinalApplicationStatus.FAILED.ordinal());
            updateMeta.setFinishedTime(System.currentTimeMillis());
            SparkJobUtils.updateApplicationMeta(applicationMeta.getApplicationId(), session, updateMeta.build());
        }
    }

    /**
     * search for cupid spark instanceId via SparkApplicationName
     * @throws ODPSConsoleException
     */
    private void searchSparkJob() throws ODPSConsoleException {
        CupidSession session = new CupidSession(cupidConf);

        CupidTaskParamProtos.ApplicationMetaList applicationMetaList =
            SparkJobUtils.getApplicationMetaList(session);

        for (CupidTaskParamProtos.ApplicationMeta applicationMeta : applicationMetaList.getApplicationMetaListList())
        {
            Pattern pattern = Pattern.compile(this.applicationName, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(applicationMeta.getApplicationName());

            if (matcher.find() || applicationMeta.getApplicationName()
                    .equalsIgnoreCase(this.applicationName)
                )
            {
                getWriter().writeError(String.format("instanceId: %s, appName: %s",
                    applicationMeta.getInstanceId(),
                    applicationMeta.getApplicationName())
                );
            }
        }
    }

    /**
     * generate spark job logview and jobview
     * @throws ODPSConsoleException
     * @throws OdpsException
     */
    private void generateSparkViews() throws ODPSConsoleException, OdpsException {

        this.getWriter().writeError(String.format("Some env might need to set following flags.\nset %s=****\nset %s=****\n",
            CupidConstants.CUPID_CONF_ODPS_MOYE_TRACKURL_HOST, CupidConstants.CUPID_CONF_ODPS_CUPID_WEBPROXY_ENDPOINT));

        String jobview;
        String logview;
        jobview = SparkJobUtils.jobViewService(this.getContext(), this.instanceId, this.applicationId);
        logview = SparkJobUtils.logViewService(this.getContext(), this.instanceId, this.applicationId);

        this.getWriter().writeError(String.format("jobview:\n%s", jobview));
        this.getWriter().writeError(String.format("logview:\n%s", logview));
    }
}
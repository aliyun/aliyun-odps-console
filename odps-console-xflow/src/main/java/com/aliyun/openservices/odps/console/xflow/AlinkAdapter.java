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

package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.*;
import com.aliyun.odps.XFlows.XFlowInstance;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandExecutor;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.PluginUtil;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AlinkAdapter {
    private static final String TEMP_RESOURCE_PREFIX = "file:";
    private static final String ALGO_PUBLIC_PROJECT = "algo_public";
    private static final String ALINK_ADAPTER_CONTEXT_RES_NAME = "alink_adapter_context.json";
    private static final String ALINK_VERSION_OPT = "alink_version";
    private static final String FLINK_VERSION_PLACEHOLDER = "${FlinkVersion}";
    private static final String ALINK_MAJOR_VERSION_PLACEHOLDER = "${AlinkMajorVersion}";
    private static final String ALINK_MINOR_VERSION_PLACEHOLDER = "${AlinkMinorVersion}";
    private static final String ODPS_SCHEME = "odps://";
    private static final String ODPS_CONSOLE_ROOT_DIR = PluginUtil.getRootPath();

    private static final String FLINK_CONSOLE_RPM_DIR = "/opt/taobao/tbdpapp/pai_alink_flink_console/";
    private static final String ALINK_BASE_RPM_DIR = "/opt/taobao/tbdpapp/pai_alink_base/";

    private static final String FLINK_CONSOLE_LOCAL_DIR = ODPS_CONSOLE_ROOT_DIR + "/.alink_root/flink_console/";
    private static final String ALINK_BASE_LOCAL_DIR = ODPS_CONSOLE_ROOT_DIR + "/.alink_root/alink_base/";
    private static final String ALINK_ALGO_LOCAL_DIR = ODPS_CONSOLE_ROOT_DIR + "/.alink_root/alink_algo/";

    private ExecutionContext paiContext;
    private Odps odps;
    private CommandLine commandLine;
    private String algoProject;
    private AlinkAdapterContext context;
    private AlinkAdapterContext.TransformAlgorithmsConfig transformConfig;
    private String flinkVersion;
    private String alinkMajorVersion;
    private String alinkMinorVersion;

    public AlinkAdapter(ExecutionContext paiContext, Odps odps, CommandLine commandLine) throws ODPSConsoleException {
        this.paiContext = paiContext;
        this.odps = odps;
        this.commandLine = commandLine;

        if (commandLine.hasOption("project")) {
            this.algoProject = commandLine.getOptionValue("project");
        } else {
            this.algoProject = ALGO_PUBLIC_PROJECT;
        }

        try {
            this.context = AlinkAdapterContext.load(paiContext, odps, algoProject, ALINK_ADAPTER_CONTEXT_RES_NAME);
        } catch (OdpsException e) {
            // do nothing
        } catch (IOException e) {
            // do nothing
        }

        if (context == null || context.getTransformAlgorithmsConfig().isEmpty()) {
            transformConfig= null;
            paiContext.getOutputWriter().writeDebug("TransformConfig is empty.");
            return;
        }

        String xflowName = commandLine.getOptionValue("name");
        transformConfig = context.getTransAlgorithmsConfig(xflowName);
        if (transformConfig == null) {
            paiContext.getOutputWriter().writeDebug("No need to transform " + xflowName + " to alink xflow.");
            return;
        }

        flinkVersion = context.getDefaultFlinkVersion();
        alinkMajorVersion = context.getDefaultAlinkMajorVersion();
        alinkMinorVersion = context.getDefaultAlinkMinorVersion();
        if (commandLine.hasOption(ALINK_VERSION_OPT)) {
            String alinkVersion = commandLine.getOptionValue(ALINK_VERSION_OPT);
            String[] versions = alinkVersion.split("-");
            if (versions.length != 3) {
                String errMsg = "Invalid alink version:" + alinkVersion
                        + ", the format should be like `1.9.0-v1.0-v1.0`.";
                paiContext.getOutputWriter().writeError(errMsg);
                throw new ODPSConsoleException(errMsg);
            }
            paiContext.getOutputWriter().writeDebug("Specified alink version:" + alinkVersion);
            flinkVersion = versions[0].trim();
            alinkMajorVersion = versions[1].trim();
            alinkMinorVersion = versions[2].trim();
        }
    }

    public  static void uncompressTarGZ(File tarFile, File dest) throws IOException {
        TarArchiveInputStream tarIn = null;
        tarIn = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(tarFile))));

        TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
        // tarIn is a TarArchiveInputStream
        while (tarEntry != null) { // create a file with the same name as the tarEntry
            File destPath = new File(dest, tarEntry.getName());
            if (tarEntry.isDirectory()) {
                destPath.mkdirs();
            } else {
                destPath.createNewFile();
                byte [] btoRead = new byte[1024];
                BufferedOutputStream bout =
                        new BufferedOutputStream(new FileOutputStream(destPath));
                int len = 0;

                while((len = tarIn.read(btoRead)) != -1) {
                    bout.write(btoRead,0,len);
                }

                bout.close();
                btoRead = null;
            }
            tarEntry = tarIn.getNextTarEntry();
        }
        tarIn.close();
    }

    public String getOrDownloadOdpsResource(String resourcePath, String localDir) throws ODPSConsoleException, OdpsException {
        return getOrDownloadOdpsResource(resourcePath, localDir, false);
    }

    public String getOrDownloadOdpsResource(String resourcePath, String localDir, boolean needUncompress) throws ODPSConsoleException, OdpsException {
        String resPath = resourcePath.replaceFirst(ODPS_SCHEME, "");
        String project = resPath.substring(0, resPath.indexOf('/'));
        String resName = resPath.substring(resPath.lastIndexOf('/') + 1);

        String resLocalPath = localDir + "/" + resName;
        if (pathExists(resLocalPath)) {
            getLogWriter().writeDebug("Resource " + resLocalPath + " already exists, no need to download.");
            return resLocalPath;
        }

        getLogWriter().writeDebug("Download odps resource " + resourcePath
                + " to " + resLocalPath + ", project:" + project + ", resourceName:" + resName + ".");

        File localDirEntry = new File(localDir);
        localDirEntry.mkdirs();

        InputStream resStream = odps.resources().getResourceAsStream(project, resName);
        try {
            FileUtil.saveInputStreamToFile(resStream, resLocalPath);
        } catch (Exception e) {
            throw new OdpsException(e);
        }
        if (needUncompress) {
            File flinkTarGzFile = new File(resLocalPath);
            File outputDir = new File(localDir);
            try {
                getLogWriter().writeDebug("Uncompress " + flinkTarGzFile + " to " + outputDir);
                uncompressTarGZ(flinkTarGzFile, outputDir);
            } catch (Exception e) {
                throw new ODPSConsoleException(e);
            }
        }

        return resLocalPath;
    }

    public boolean needTransform() {
        if (transformConfig ==  null) {
            return false;
        }

        String runningProject = odps.getDefaultProject();
        if (transformConfig.getProjectList() == null
                || transformConfig.getProjectList().isEmpty()
                || transformConfig.getProjectList().contains(runningProject)) {
            return true;
        }

        return false;
    }

    public XFlowInstance createXflowInstance(
            AlinkTransformer.AlgoJob algoJob,
            String cupidTaskPlanContent,
            String userSettings) throws ODPSConsoleException, OdpsException {
        XFlowInstance xFlowInstance = new XFlowInstance();

        xFlowInstance.setXflowName(algoJob.algoName);
        xFlowInstance.setProject(algoProject);
        String guid = UUID.randomUUID().toString();
        xFlowInstance.setGuid(guid);

        Integer priority = paiContext.getPaiPriority();
        xFlowInstance.setPriority(priority);

        for (Map.Entry<String, String> property : algoJob.params.entrySet()) {
            String value = property.getValue().toString();
            if (value.toLowerCase().startsWith(TEMP_RESOURCE_PREFIX)) {
                try {
                    value = new URL(URLDecoder.decode(value, "utf-8")).getPath();
                } catch (IOException e) {
                    throw new ODPSConsoleException("Invalid temp fileName:" + e.getMessage(), e);
                }
                value = odps.resources().createTempResource(odps.getDefaultProject(), value).getName();
            }
            xFlowInstance.setParameter(property.getKey().toString(), value);
        }

        xFlowInstance.setParameter("cupidTaskPlan", cupidTaskPlanContent);
        if (!userSettings.isEmpty()) {
            xFlowInstance.setParameter("userSettings", userSettings);
        }

        HashMap<String, String> userConfig = getUserConfig(commandLine);
        for (Map.Entry<String, String> property : userConfig.entrySet()) {
            xFlowInstance.setProperty(property.getKey(), property.getValue());
        }

        return xFlowInstance;
    }

    private DefaultOutputWriter getLogWriter() {
        return paiContext.getOutputWriter();
    }

    public String createTempCommandFile(String command) throws ODPSConsoleException {
        try {
            File tempCommandFile = File.createTempFile("alink_command_", ".sh");
            tempCommandFile.deleteOnExit();
            FileOutputStream outputStream = new FileOutputStream(tempCommandFile);
            outputStream.write(command.getBytes());
            outputStream.close();
            return tempCommandFile.getAbsolutePath();
        } catch (IOException e) {
            throw new ODPSConsoleException(e);
        }
    }

    private String ExecuteShell(String command) throws ODPSConsoleException {
        try {
            CommandExecutor.ExecutorResult result = CommandExecutor.run(command, false);
            if (result.getEcode() != 0) {
                throw new ODPSConsoleException("Executor command, error code:"
                        + result.getEcode() + ", error:" + result.getErrorStr());
            }
            return result.getOutStr();
        } catch (IOException e) {
            throw new ODPSConsoleException(e);
        }
    }

    public final static Pattern cupidPattern  = Pattern.compile(
            "CUPID_TASK_PLAN_INFO:(?<plan>[^\n]+?)\n",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public void writeFlinkConfFiles(
            AlinkTransformer.FlinkJob flinkJob,
            Map<String, String> defaultConfFiles,
            String localConfDir) throws ODPSConsoleException {
        try {
            File tmpConfDir = new File(localConfDir);
            if (!tmpConfDir.exists()) {
                tmpConfDir.mkdirs();
            }
            tmpConfDir.deleteOnExit();

            Map<String, String> allConfFiles = new HashMap<String, String>();
            if (flinkJob.confFiles != null) {
                allConfFiles.putAll(flinkJob.confFiles);
            }

            if (defaultConfFiles != null) {
                allConfFiles.putAll(defaultConfFiles);
            }

            if (allConfFiles.isEmpty()) {
                getLogWriter().writeDebug("No flink conf files to write");
                return;
            }

            for (Map.Entry<String, String> entry : allConfFiles.entrySet()) {
                String confFilePath = localConfDir + entry.getKey();
                getLogWriter().writeDebug("Write flink conf file:" + confFilePath);
                File confFile = new File(confFilePath);
                if (confFile.createNewFile()) {
                    confFile.deleteOnExit();
                    FileOutputStream outputStream = new FileOutputStream(confFile);
                    outputStream.write(entry.getValue().getBytes());
                    outputStream.close();
                }
            }
        } catch (IOException e) {
            throw new ODPSConsoleException(e);
        }
    }

    public String generateCupidTaskPlanContent(
            AlinkTransformer.FlinkJobList flinkJobList,
            String alinkBaseJarFilePath,
            String alinkBaseJarDir,
            String alinkAlgoJarFilePath,
            String flinkConsoleLocalDir) throws ODPSConsoleException, OdpsException {

        String flinkLogPath = FLINK_CONSOLE_LOCAL_DIR + "log/";
        File flinkLogDir = new File(flinkLogPath);
        if (!flinkLogDir.exists()) {
            flinkLogDir.mkdirs();
        }

        String cupidTaskPlanContent = "";
        for (AlinkTransformer.FlinkJob job : flinkJobList.flinkJobs) {
            String tmpFlinkConfDir = FLINK_CONSOLE_LOCAL_DIR + UUID.randomUUID().toString() + "/";
            writeFlinkConfFiles(job, flinkJobList.defaultConfFiles, tmpFlinkConfDir);

            StringBuilder cmdBuilder = new StringBuilder();
            cmdBuilder.append("cd " + flinkConsoleLocalDir + " && ");

            Map<String, String> allEnvs = new HashMap<String, String>();
            getLogWriter().writeDebug("JAVA_HOME = " + System.getenv("JAVA_HOME"));
            allEnvs.put("JAVA_HOME", System.getenv("JAVA_HOME"));
            allEnvs.put("FLINK_LOG_DIR", flinkLogPath);
            allEnvs.put("FLINK_CONF_DIR", tmpFlinkConfDir);
            allEnvs.put("HADOOP_CLASSPATH", alinkBaseJarFilePath + ":" + tmpFlinkConfDir);
            allEnvs.put("CUPID_DRY_RUN_MODE_ENABLE", "true");
            if (job.envs != null) {
                allEnvs.putAll(job.envs);
            }

            for (Map.Entry<String, String> env: allEnvs.entrySet()) {
                cmdBuilder.append(env.getKey() + "=" + env.getValue() + " ");
            }

            cmdBuilder.append("./bin/flink run ");
            cmdBuilder.append("-yt " + alinkBaseJarDir + " ");

            for (String param : job.cupidParams) {
                cmdBuilder.append(param + " ");
            }

            cmdBuilder.append("-d " + alinkAlgoJarFilePath + " ");

            for (String param : job.alinkParams) {
                cmdBuilder.append(param + " ");
            }

            getLogWriter().writeError("Flink console is running...");
            getLogWriter().writeDebug("Flink command:" + cmdBuilder.toString());

            String cmdFile = createTempCommandFile(cmdBuilder.toString());
            String result = ExecuteShell("/bin/bash " + cmdFile);
            Matcher m = cupidPattern.matcher(result);
            if (m.find()) {
                cupidTaskPlanContent += m.group("plan");
                cupidTaskPlanContent += ";";
            }
            getLogWriter().writeError("Flink console execute success.");
        }

        if (!cupidTaskPlanContent.isEmpty()) {
            cupidTaskPlanContent = cupidTaskPlanContent.substring(0, cupidTaskPlanContent.length() - 1);
        }

        getLogWriter().writeDebug("cupidTaskPlanContent:" + cupidTaskPlanContent);

        return cupidTaskPlanContent;
    }

    public static HashMap<String, String> getUserConfig(CommandLine cl) {
        // get session config
        HashMap<String, String> userConfig = new HashMap<String, String>();

        String jobName = cl.getOptionValue("jobname");
        HashMap<String, String> settings
                = new HashMap<String, String>(SetCommand.setMap);

        if (jobName != null) {
            settings.put("odps.task.workflow.custom_job_name", jobName);
        }

        if (!settings.isEmpty()) {
            userConfig.put("settings", new GsonBuilder().disableHtmlEscaping().create()
                    .toJson(settings));
        }
        return userConfig;
    }

    public String generateCupidTaskSettings(AlinkTransformer.FlinkJob[] flinkJobs) {
        String settings = "";
        for (AlinkTransformer.FlinkJob job : flinkJobs) {
            String setting = "";
            for (Map.Entry<String, String> entry : job.confFiles.entrySet()) {
                String confContents = entry.getValue();
                String[] confKvs = confContents.split("\n");

                for (String kv : confKvs) {
                    if (kv.startsWith("odps.") && !kv.startsWith("odps.access.")) {
                        // the format of each item in confFile content is `key: value`
                        if (kv.contains(": ")) {
                            setting += kv.replaceFirst(": ", "=") + ",";
                        } else {
                            setting += kv + ",";
                        }
                    }
                }
            }
            if (!setting.isEmpty()) {
                setting = setting.substring(0, setting.length() - 1);
            }
            settings += setting + ";";
        }

        if (!settings.isEmpty()) {
            settings = settings.substring(0, settings.length() - 1);
        }

        return settings;
    }

    private static boolean pathExists(String filePath) {
        return new File(filePath).exists();
    }

    private static String getAbsolutePath(String relativePath) {
        return new File(relativePath).getAbsolutePath();
    }

    private String getFlinkConsoleRootDir() throws ODPSConsoleException, OdpsException {
        String flinkConsoleRootDirName = "flink-" + flinkVersion + "-odps/";
        String flinkConsoleRpmPath = FLINK_CONSOLE_RPM_DIR + flinkConsoleRootDirName;

        if (pathExists(flinkConsoleRpmPath)) {
            getLogWriter().writeDebug("Find flink console " + flinkConsoleRpmPath + " in rpm dir.");
            return flinkConsoleRpmPath;
        } else {
            getLogWriter().writeDebug("Flink console " + flinkConsoleRpmPath
                    + " not found in rpm dir, download from odps resource.");
        }

        String flinkConsoleResPath = context.getDefaultFlinkResource().replace(FLINK_VERSION_PLACEHOLDER, flinkVersion);
        String flinkConsoleLocalDir = getAbsolutePath(FLINK_CONSOLE_LOCAL_DIR + flinkVersion);
        getOrDownloadOdpsResource(flinkConsoleResPath, flinkConsoleLocalDir, true);

        String flinkConsoleRootDir = flinkConsoleLocalDir + File.separator + flinkConsoleRootDirName;
        if (!pathExists(flinkConsoleRootDir)) {
            throw new ODPSConsoleException("Flink console " + flinkConsoleRootDir + " not found.");
        }

        getLogWriter().writeDebug("Flink console root dir: " + flinkConsoleRootDir + ".");

        String chmodCmd = "chmod +x " + flinkConsoleRootDir + "/bin/flink";
        ExecuteShell(chmodCmd);

        return flinkConsoleRootDir;
    }

    private String getAlinkBaseJarFilePath() throws ODPSConsoleException, OdpsException{
        String alinkBaseJarRpmPath = ALINK_BASE_RPM_DIR + "alink-base-" + alinkMajorVersion + ".jar";

        if (pathExists(alinkBaseJarRpmPath)) {
            getLogWriter().writeDebug("Find alink base jar " + alinkBaseJarRpmPath + " in rpm dir.");
            return alinkBaseJarRpmPath;
        } else {
            getLogWriter().writeDebug("Alink base jar " + alinkBaseJarRpmPath
                    + " not found in rpm dir, download from odps resource.");
        }

        String alinkBaseResPath = context.getDefaultAlinkBaseResource().replace(ALINK_MAJOR_VERSION_PLACEHOLDER, alinkMajorVersion);
        String alinkBaseLocalDir = getAbsolutePath(ALINK_BASE_LOCAL_DIR + alinkMajorVersion);

        return getOrDownloadOdpsResource(alinkBaseResPath, alinkBaseLocalDir);
    }

    private String getAlinkAlgoJarFilePath() throws ODPSConsoleException, OdpsException {
        String alinkAlgoResPath = context.getDefaultAlinkAlgoResource()
                .replace(FLINK_VERSION_PLACEHOLDER, flinkVersion)
                .replace(ALINK_MAJOR_VERSION_PLACEHOLDER, alinkMajorVersion)
                .replace(ALINK_MINOR_VERSION_PLACEHOLDER, alinkMinorVersion);
        String alinkAlgoLocalDir = getAbsolutePath(ALINK_ALGO_LOCAL_DIR + flinkVersion
                + "-" + alinkMajorVersion + "-" + alinkMinorVersion);

        return getOrDownloadOdpsResource(alinkAlgoResPath, alinkAlgoLocalDir);
    }

    public XFlowInstance createAlinkXflowInstance() throws ODPSConsoleException, OdpsException {
        if (!needTransform()) {
            return null;
        }

        String flinkConsoleRootDir = getFlinkConsoleRootDir();

        String alinkBaseJarFilePath = getAlinkBaseJarFilePath();
        String alinkBaseLocalDir = alinkBaseJarFilePath.substring(0, alinkBaseJarFilePath.lastIndexOf('/'));

        String alinkAlgoJarFilePath = getAlinkAlgoJarFilePath();

        AlinkTransformer alinkTransformer = new AlinkTransformer();
        try {
             alinkTransformer.generateFlinkJobs(
                     paiContext, commandLine, flinkConsoleRootDir,
                     alinkBaseJarFilePath, alinkAlgoJarFilePath);
        } catch (Exception e) {
            throw new ODPSConsoleException(e);
        }
        AlinkTransformer.FlinkJobList flinkJobList = alinkTransformer.getFlinkJobList();

        String cupidPlanContent = generateCupidTaskPlanContent(
                flinkJobList, alinkBaseJarFilePath,
                alinkBaseLocalDir, alinkAlgoJarFilePath, flinkConsoleRootDir);

        String userSettings = generateCupidTaskSettings(flinkJobList.flinkJobs);

        return createXflowInstance(flinkJobList.xflowJob, cupidPlanContent, userSettings);
    }
}

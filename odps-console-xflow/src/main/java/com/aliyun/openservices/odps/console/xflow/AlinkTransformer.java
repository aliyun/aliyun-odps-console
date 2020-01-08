package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AlinkTransformer {
    public class OdpsConfig {
        String accessId;
        String accessKey;
        String endPoint;
        String projectName;
    }

    public class AlgoJob {
        String algoName;

        Map<String, String> params;

        OdpsConfig odpsConfig;
    }

    public class FlinkJob {
        Map<String, String> confFiles;
        List<String> cupidParams;
        List<String> alinkParams;
        Map<String, String> envs;
    }

    public class FlinkJobList {
        //xflow job.
        AlgoJob xflowJob;
        //flink job.
        FlinkJob[] flinkJobs;
        Map<String, String> defaultConfFiles;
    }

    private FlinkJobList flinkJobList;

    public FlinkJobList getFlinkJobList() { return flinkJobList; }

    public String constructAlgoJobJson(ExecutionContext paiContext, CommandLine commandLine) {
        AlgoJob algoJob = new AlgoJob();
        algoJob.algoName = commandLine.getOptionValue("name");

        Properties properties = commandLine.getOptionProperties("D");
        algoJob.params = new HashMap<String, String>();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            String value = property.getValue().toString();
            algoJob.params.put(property.getKey().toString(), value);
        }

        algoJob.odpsConfig = new OdpsConfig();
        algoJob.odpsConfig.accessId = paiContext.getAccessId();
        algoJob.odpsConfig.accessKey = paiContext.getAccessKey();
        algoJob.odpsConfig.endPoint = paiContext.getEndpoint();
        algoJob.odpsConfig.projectName = paiContext.getProjectName();

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(algoJob);
    }

    public void generateFlinkJobs(
            ExecutionContext paiContext,
            CommandLine commandLine,
            String flinkConsoleRootDir,
            String alinkBaseJarPath,
            String alinkAlgoJarPath) throws Exception {
        String algoJobJson = constructAlgoJobJson(paiContext, commandLine);

        if (flinkConsoleRootDir == null) {
            throw new ODPSConsoleException("Flink console root dir " + flinkConsoleRootDir +  " not found.");
        }

        String flinkConsoleLibDir = flinkConsoleRootDir + "/lib/";
        List<File> files = (List<File>) FileUtils.listFiles(new File(flinkConsoleLibDir), null, true);
        files.add(0, new File(alinkBaseJarPath));
        files.add(0, new File(alinkAlgoJarPath));

        URL[] urls = new URL[files.size()];
        int i = 0;
        for (File file : files) {
            String filePath = "file://" + file.getAbsolutePath();
            urls[i++] = new URL(filePath).toURI().toURL();
            paiContext.getOutputWriter().writeDebug("Find jar file " + filePath + ".");
        }

        URLClassLoader child = new URLClassLoader(urls);
        Class classToLoad = Class.forName("com.alibaba.alink.executor.pai.PaiMain", true, child);
        Method method = classToLoad.getDeclaredMethod("getFlinkJobsJson", String.class);
        Object instance = classToLoad.getDeclaredConstructor().newInstance();
        Object result = method.invoke(instance, algoJobJson);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        flinkJobList = gson.fromJson((String)result, FlinkJobList.class);

        // print alink version
        method = classToLoad.getDeclaredMethod("getVersion");
        instance = classToLoad.getDeclaredConstructor().newInstance();
        result = method.invoke(instance);
        paiContext.getOutputWriter().writeError("Alink version:" + (String)result);
    }
}

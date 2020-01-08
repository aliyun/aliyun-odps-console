package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class AlinkAdapterContext {
    @SerializedName("DefaultFlinkVersion")
    private String defaultFlinkVersion;

    @SerializedName("DefaultAlinkMajorVersion")
    private String defaultAlinkMajorVersion;

    @SerializedName("DefaultAlinkMinorVersion")
    private String defaultAlinkMinorVersion;

    @SerializedName("DefaultFlinkResource")
    private String defaultFlinkResource;

    @SerializedName("DefaultAlinkBaseResource")
    private String defaultAlinkBaseResource;

    @SerializedName("DefaultAlinkAlgoResource")
    private String defaultAlinkAlgoResource;

    public String getDefaultFlinkVersion() { return defaultFlinkVersion; }

    public String getDefaultAlinkMajorVersion() { return defaultAlinkMajorVersion; }

    public String getDefaultAlinkMinorVersion() { return defaultAlinkMinorVersion; }

    public String getDefaultFlinkResource() { return defaultFlinkResource; }

    public String getDefaultAlinkBaseResource() { return defaultAlinkBaseResource; }

    public String getDefaultAlinkAlgoResource() { return defaultAlinkAlgoResource; }

    public class TransformAlgorithmsConfig{
        @SerializedName("XflowAlgoName")
        private String xflowAlgoName;

        @SerializedName("ProjectList")
        private List<String> projectList = new ArrayList<String>();

        public String getXflowAlgoName() { return xflowAlgoName; }

        public List<String> getProjectList() { return projectList; }
    }

    @SerializedName("TransformConfig")
    private List<TransformAlgorithmsConfig> transformAlgorithmsConfig = new ArrayList<TransformAlgorithmsConfig>();

    public List<TransformAlgorithmsConfig> getTransformAlgorithmsConfig() {
        return transformAlgorithmsConfig;
    }

    public TransformAlgorithmsConfig getTransAlgorithmsConfig(String algoName) {
        for (TransformAlgorithmsConfig config: transformAlgorithmsConfig) {
            if (config.xflowAlgoName.toUpperCase().equals(algoName.toUpperCase())) {
                return config;
            }
        }
        return null;
    }

    public static AlinkAdapterContext load(ExecutionContext paiContext, Odps odps, String project, String contextResourceName) throws OdpsException, IOException {
        AlinkAdapterContext context = null;
        try {
            InputStream jsonStream = odps.resources().getResourceAsStream(project, contextResourceName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(jsonStream));
            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            context = gson.fromJson(reader, AlinkAdapterContext.class);
        } catch (Exception e) {
            // log here and ignore this exception
            paiContext.getOutputWriter().writeDebug("Load alink adapter context failed:" + e.getMessage());
        }

        return context;
    }
}

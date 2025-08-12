// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchQueryRequest extends TeaModel {
    // PAI-Xflow名称
    @NameInMap("AlgoName")
    public String algoName;

    // PAI-project名称
    @NameInMap("ProjectName")
    public String projectName;

    // properties of pai command
    @NameInMap("Properties")
    public java.util.Map<String, String> properties;

    // odps settings
    @NameInMap("Settings")
    public java.util.Map<String, String> settings;

    public static JobDispatchQueryRequest build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchQueryRequest self = new JobDispatchQueryRequest();
        return TeaModel.build(map, self);
    }

    public JobDispatchQueryRequest setAlgoName(String algoName) {
        this.algoName = algoName;
        return this;
    }
    public String getAlgoName() {
        return this.algoName;
    }

    public JobDispatchQueryRequest setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }
    public String getProjectName() {
        return this.projectName;
    }

    public JobDispatchQueryRequest setProperties(java.util.Map<String, String> properties) {
        this.properties = properties;
        return this;
    }
    public java.util.Map<String, String> getProperties() {
        return this.properties;
    }

    public JobDispatchQueryRequest setSettings(java.util.Map<String, String> settings) {
        this.settings = settings;
        return this;
    }
    public java.util.Map<String, String> getSettings() {
        return this.settings;
    }

}

// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchQueryShrinkRequest extends TeaModel {
    // PAI-Xflow名称
    @NameInMap("AlgoName")
    public String algoName;

    // PAI-project名称
    @NameInMap("ProjectName")
    public String projectName;

    // properties of pai command
    @NameInMap("Properties")
    public String propertiesShrink;

    // odps settings
    @NameInMap("Settings")
    public String settingsShrink;

    public static JobDispatchQueryShrinkRequest build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchQueryShrinkRequest self = new JobDispatchQueryShrinkRequest();
        return TeaModel.build(map, self);
    }

    public JobDispatchQueryShrinkRequest setAlgoName(String algoName) {
        this.algoName = algoName;
        return this;
    }
    public String getAlgoName() {
        return this.algoName;
    }

    public JobDispatchQueryShrinkRequest setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }
    public String getProjectName() {
        return this.projectName;
    }

    public JobDispatchQueryShrinkRequest setPropertiesShrink(String propertiesShrink) {
        this.propertiesShrink = propertiesShrink;
        return this;
    }
    public String getPropertiesShrink() {
        return this.propertiesShrink;
    }

    public JobDispatchQueryShrinkRequest setSettingsShrink(String settingsShrink) {
        this.settingsShrink = settingsShrink;
        return this;
    }
    public String getSettingsShrink() {
        return this.settingsShrink;
    }

}

// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobWorkflowShrinkRequest extends TeaModel {
    @NameInMap("AlgoName")
    public String algoName;

    @NameInMap("ProjectName")
    public String projectName;

    @NameInMap("Properties")
    public String propertiesShrink;

    public static GetJobWorkflowShrinkRequest build(java.util.Map<String, ?> map) throws Exception {
        GetJobWorkflowShrinkRequest self = new GetJobWorkflowShrinkRequest();
        return TeaModel.build(map, self);
    }

    public GetJobWorkflowShrinkRequest setAlgoName(String algoName) {
        this.algoName = algoName;
        return this;
    }
    public String getAlgoName() {
        return this.algoName;
    }

    public GetJobWorkflowShrinkRequest setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }
    public String getProjectName() {
        return this.projectName;
    }

    public GetJobWorkflowShrinkRequest setPropertiesShrink(String propertiesShrink) {
        this.propertiesShrink = propertiesShrink;
        return this;
    }
    public String getPropertiesShrink() {
        return this.propertiesShrink;
    }

}

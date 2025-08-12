// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobWorkflowRequest extends TeaModel {
    @NameInMap("AlgoName")
    public String algoName;

    @NameInMap("ProjectName")
    public String projectName;

    @NameInMap("Properties")
    public java.util.Map<String, String> properties;

    public static GetJobWorkflowRequest build(java.util.Map<String, ?> map) throws Exception {
        GetJobWorkflowRequest self = new GetJobWorkflowRequest();
        return TeaModel.build(map, self);
    }

    public GetJobWorkflowRequest setAlgoName(String algoName) {
        this.algoName = algoName;
        return this;
    }
    public String getAlgoName() {
        return this.algoName;
    }

    public GetJobWorkflowRequest setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }
    public String getProjectName() {
        return this.projectName;
    }

    public GetJobWorkflowRequest setProperties(java.util.Map<String, String> properties) {
        this.properties = properties;
        return this;
    }
    public java.util.Map<String, String> getProperties() {
        return this.properties;
    }

}

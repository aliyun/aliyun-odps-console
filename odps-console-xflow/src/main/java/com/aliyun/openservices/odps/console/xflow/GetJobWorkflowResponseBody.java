// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobWorkflowResponseBody extends TeaModel {
    @NameInMap("RequestId")
    public String requestId;

    @NameInMap("Workflow")
    public String workflow;

    public static GetJobWorkflowResponseBody build(java.util.Map<String, ?> map) throws Exception {
        GetJobWorkflowResponseBody self = new GetJobWorkflowResponseBody();
        return TeaModel.build(map, self);
    }

    public GetJobWorkflowResponseBody setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }
    public String getRequestId() {
        return this.requestId;
    }

    public GetJobWorkflowResponseBody setWorkflow(String workflow) {
        this.workflow = workflow;
        return this;
    }
    public String getWorkflow() {
        return this.workflow;
    }

}

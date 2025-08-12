// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchQueryResponseBody extends TeaModel {
    // 作业Id
    @NameInMap("JobId")
    public String jobId;

    // 请求Id
    @NameInMap("RequestId")
    public String requestId;

    public static JobDispatchQueryResponseBody build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchQueryResponseBody self = new JobDispatchQueryResponseBody();
        return TeaModel.build(map, self);
    }

    public JobDispatchQueryResponseBody setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }
    public String getJobId() {
        return this.jobId;
    }

    public JobDispatchQueryResponseBody setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }
    public String getRequestId() {
        return this.requestId;
    }

}

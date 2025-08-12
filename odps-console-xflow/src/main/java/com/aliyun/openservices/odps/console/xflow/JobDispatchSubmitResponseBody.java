// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchSubmitResponseBody extends TeaModel {
    // 作业Url
    @NameInMap("JobUrl")
    public String jobUrl;

    public static JobDispatchSubmitResponseBody build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchSubmitResponseBody self = new JobDispatchSubmitResponseBody();
        return TeaModel.build(map, self);
    }

    public JobDispatchSubmitResponseBody setJobUrl(String jobUrl) {
        this.jobUrl = jobUrl;
        return this;
    }
    public String getJobUrl() {
        return this.jobUrl;
    }

}

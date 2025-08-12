// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchSubmitResponse extends TeaModel {
    @NameInMap("headers")
    @Validation(required = true)
    public java.util.Map<String, String> headers;

    @NameInMap("body")
    @Validation(required = true)
    public JobDispatchSubmitResponseBody body;

    public static JobDispatchSubmitResponse build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchSubmitResponse self = new JobDispatchSubmitResponse();
        return TeaModel.build(map, self);
    }

    public JobDispatchSubmitResponse setHeaders(java.util.Map<String, String> headers) {
        this.headers = headers;
        return this;
    }
    public java.util.Map<String, String> getHeaders() {
        return this.headers;
    }

    public JobDispatchSubmitResponse setBody(JobDispatchSubmitResponseBody body) {
        this.body = body;
        return this;
    }
    public JobDispatchSubmitResponseBody getBody() {
        return this.body;
    }

}

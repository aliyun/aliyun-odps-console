// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobDispatchQueryResponse extends TeaModel {
    @NameInMap("headers")
    @Validation(required = true)
    public java.util.Map<String, String> headers;

    @NameInMap("body")
    @Validation(required = true)
    public JobDispatchQueryResponseBody body;

    public static JobDispatchQueryResponse build(java.util.Map<String, ?> map) throws Exception {
        JobDispatchQueryResponse self = new JobDispatchQueryResponse();
        return TeaModel.build(map, self);
    }

    public JobDispatchQueryResponse setHeaders(java.util.Map<String, String> headers) {
        this.headers = headers;
        return this;
    }
    public java.util.Map<String, String> getHeaders() {
        return this.headers;
    }

    public JobDispatchQueryResponse setBody(JobDispatchQueryResponseBody body) {
        this.body = body;
        return this;
    }
    public JobDispatchQueryResponseBody getBody() {
        return this.body;
    }

}

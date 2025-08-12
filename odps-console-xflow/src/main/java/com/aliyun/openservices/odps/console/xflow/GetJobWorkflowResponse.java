// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobWorkflowResponse extends TeaModel {
    @NameInMap("headers")
    @Validation(required = true)
    public java.util.Map<String, String> headers;

    @NameInMap("body")
    @Validation(required = true)
    public GetJobWorkflowResponseBody body;

    public static GetJobWorkflowResponse build(java.util.Map<String, ?> map) throws Exception {
        GetJobWorkflowResponse self = new GetJobWorkflowResponse();
        return TeaModel.build(map, self);
    }

    public GetJobWorkflowResponse setHeaders(java.util.Map<String, String> headers) {
        this.headers = headers;
        return this;
    }
    public java.util.Map<String, String> getHeaders() {
        return this.headers;
    }

    public GetJobWorkflowResponse setBody(GetJobWorkflowResponseBody body) {
        this.body = body;
        return this;
    }
    public GetJobWorkflowResponseBody getBody() {
        return this.body;
    }

}

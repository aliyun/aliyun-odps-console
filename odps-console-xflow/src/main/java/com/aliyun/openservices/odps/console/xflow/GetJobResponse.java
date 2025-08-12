// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobResponse extends TeaModel {
    @NameInMap("headers")
    @Validation(required = true)
    public java.util.Map<String, String> headers;

    @NameInMap("body")
    @Validation(required = true)
    public GetJobResponseBody body;

    public static GetJobResponse build(java.util.Map<String, ?> map) throws Exception {
        GetJobResponse self = new GetJobResponse();
        return TeaModel.build(map, self);
    }

    public GetJobResponse setHeaders(java.util.Map<String, String> headers) {
        this.headers = headers;
        return this;
    }
    public java.util.Map<String, String> getHeaders() {
        return this.headers;
    }

    public GetJobResponse setBody(GetJobResponseBody body) {
        this.body = body;
        return this;
    }
    public GetJobResponseBody getBody() {
        return this.body;
    }

}

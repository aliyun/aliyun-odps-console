// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class GetJobResponseBody extends TeaModel {
    // 作业Id
    @NameInMap("JobId")
    public String jobId;

    // 作业类型
    @NameInMap("JobType")
    public String jobType;

    // 作业显示名称
    @NameInMap("DisplayName")
    public String displayName;

    // 作业提交人Id
    @NameInMap("UserId")
    public String userId;

    // 作业状态
    @NameInMap("Status")
    public String status;

    // 状态详情码
    @NameInMap("ReasonCode")
    public String reasonCode;

    // 状态详情
    @NameInMap("ReasonMessage")
    public String reasonMessage;

    // 作业规格配置
    @NameInMap("JobSpecs")
    public java.util.List<JobSpec> jobSpecs;

    // 用户命令
    @NameInMap("UserCommand")
    public String userCommand;

    // 数据源配置列表
    @NameInMap("DataSources")
    public java.util.List<GetJobResponseBodyDataSources> dataSources;

    // 代码源配置
    @NameInMap("CodeSource")
    public GetJobResponseBodyCodeSource codeSource;

    // 三方库配置列表
    @NameInMap("ThirdpartyLibs")
    public java.util.List<String> thirdpartyLibs;

    // 三方库(requirements.txt)文件路径
    @NameInMap("ThirdpartyLibDir")
    public String thirdpartyLibDir;

    // 环境变量配置
    @NameInMap("Envs")
    public java.util.Map<String, String> envs;

    // 作业创建时间（UTC）
    @NameInMap("GmtCreateTime")
    public String gmtCreateTime;

    // 作业结束时间（UTC）
    @NameInMap("GmtFinishTime")
    public String gmtFinishTime;

    // 作业运行时长（s）
    @NameInMap("Duration")
    public Long duration;

    // 作业所以运行Pod列表
    @NameInMap("Pods")
    public java.util.List<GetJobResponseBodyPods> pods;

    // 请求Id
    @NameInMap("RequestId")
    public String requestId;

    public static GetJobResponseBody build(java.util.Map<String, ?> map) throws Exception {
        GetJobResponseBody self = new GetJobResponseBody();
        return TeaModel.build(map, self);
    }

    public GetJobResponseBody setJobId(String jobId) {
        this.jobId = jobId;
        return this;
    }
    public String getJobId() {
        return this.jobId;
    }

    public GetJobResponseBody setJobType(String jobType) {
        this.jobType = jobType;
        return this;
    }
    public String getJobType() {
        return this.jobType;
    }

    public GetJobResponseBody setDisplayName(String displayName) {
        this.displayName = displayName;
        return this;
    }
    public String getDisplayName() {
        return this.displayName;
    }

    public GetJobResponseBody setUserId(String userId) {
        this.userId = userId;
        return this;
    }
    public String getUserId() {
        return this.userId;
    }

    public GetJobResponseBody setStatus(String status) {
        this.status = status;
        return this;
    }
    public String getStatus() {
        return this.status;
    }

    public GetJobResponseBody setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
        return this;
    }
    public String getReasonCode() {
        return this.reasonCode;
    }

    public GetJobResponseBody setReasonMessage(String reasonMessage) {
        this.reasonMessage = reasonMessage;
        return this;
    }
    public String getReasonMessage() {
        return this.reasonMessage;
    }

    public GetJobResponseBody setJobSpecs(java.util.List<JobSpec> jobSpecs) {
        this.jobSpecs = jobSpecs;
        return this;
    }
    public java.util.List<JobSpec> getJobSpecs() {
        return this.jobSpecs;
    }

    public GetJobResponseBody setUserCommand(String userCommand) {
        this.userCommand = userCommand;
        return this;
    }
    public String getUserCommand() {
        return this.userCommand;
    }

    public GetJobResponseBody setDataSources(java.util.List<GetJobResponseBodyDataSources> dataSources) {
        this.dataSources = dataSources;
        return this;
    }
    public java.util.List<GetJobResponseBodyDataSources> getDataSources() {
        return this.dataSources;
    }

    public GetJobResponseBody setCodeSource(GetJobResponseBodyCodeSource codeSource) {
        this.codeSource = codeSource;
        return this;
    }
    public GetJobResponseBodyCodeSource getCodeSource() {
        return this.codeSource;
    }

    public GetJobResponseBody setThirdpartyLibs(java.util.List<String> thirdpartyLibs) {
        this.thirdpartyLibs = thirdpartyLibs;
        return this;
    }
    public java.util.List<String> getThirdpartyLibs() {
        return this.thirdpartyLibs;
    }

    public GetJobResponseBody setThirdpartyLibDir(String thirdpartyLibDir) {
        this.thirdpartyLibDir = thirdpartyLibDir;
        return this;
    }
    public String getThirdpartyLibDir() {
        return this.thirdpartyLibDir;
    }

    public GetJobResponseBody setEnvs(java.util.Map<String, String> envs) {
        this.envs = envs;
        return this;
    }
    public java.util.Map<String, String> getEnvs() {
        return this.envs;
    }

    public GetJobResponseBody setGmtCreateTime(String gmtCreateTime) {
        this.gmtCreateTime = gmtCreateTime;
        return this;
    }
    public String getGmtCreateTime() {
        return this.gmtCreateTime;
    }

    public GetJobResponseBody setGmtFinishTime(String gmtFinishTime) {
        this.gmtFinishTime = gmtFinishTime;
        return this;
    }
    public String getGmtFinishTime() {
        return this.gmtFinishTime;
    }

    public GetJobResponseBody setDuration(Long duration) {
        this.duration = duration;
        return this;
    }
    public Long getDuration() {
        return this.duration;
    }

    public GetJobResponseBody setPods(java.util.List<GetJobResponseBodyPods> pods) {
        this.pods = pods;
        return this;
    }
    public java.util.List<GetJobResponseBodyPods> getPods() {
        return this.pods;
    }

    public GetJobResponseBody setRequestId(String requestId) {
        this.requestId = requestId;
        return this;
    }
    public String getRequestId() {
        return this.requestId;
    }

    public static class GetJobResponseBodyDataSources extends TeaModel {
        // 数据源Id
        @NameInMap("DataSourceId")
        public String dataSourceId;

        public static GetJobResponseBodyDataSources build(java.util.Map<String, ?> map) throws Exception {
            GetJobResponseBodyDataSources self = new GetJobResponseBodyDataSources();
            return TeaModel.build(map, self);
        }

        public GetJobResponseBodyDataSources setDataSourceId(String dataSourceId) {
            this.dataSourceId = dataSourceId;
            return this;
        }
        public String getDataSourceId() {
            return this.dataSourceId;
        }

    }

    public static class GetJobResponseBodyCodeSource extends TeaModel {
        // 代码源Id
        @NameInMap("CodeSourceId")
        public String codeSourceId;

        // 代码分支
        @NameInMap("Branch")
        public String branch;

        // 代码Commit
        @NameInMap("Commit")
        public String commit;

        public static GetJobResponseBodyCodeSource build(java.util.Map<String, ?> map) throws Exception {
            GetJobResponseBodyCodeSource self = new GetJobResponseBodyCodeSource();
            return TeaModel.build(map, self);
        }

        public GetJobResponseBodyCodeSource setCodeSourceId(String codeSourceId) {
            this.codeSourceId = codeSourceId;
            return this;
        }
        public String getCodeSourceId() {
            return this.codeSourceId;
        }

        public GetJobResponseBodyCodeSource setBranch(String branch) {
            this.branch = branch;
            return this;
        }
        public String getBranch() {
            return this.branch;
        }

        public GetJobResponseBodyCodeSource setCommit(String commit) {
            this.commit = commit;
            return this;
        }
        public String getCommit() {
            return this.commit;
        }

    }

    public static class GetJobResponseBodyPods extends TeaModel {
        // Pod类型
        @NameInMap("Type")
        public String type;

        // Pod Id
        @NameInMap("PodId")
        public String podId;

        // Pod状态
        @NameInMap("Status")
        public String status;

        // Pod Ip
        @NameInMap("Ip")
        public String ip;

        // Pod创建时间（UTC）
        @NameInMap("GmtGreateTime")
        public String gmtGreateTime;

        // Pod启动时间（UTC）
        @NameInMap("GmtStartTime")
        public String gmtStartTime;

        // Pod结束时间（UTC）
        @NameInMap("GmtFinishTime")
        public String gmtFinishTime;

        public static GetJobResponseBodyPods build(java.util.Map<String, ?> map) throws Exception {
            GetJobResponseBodyPods self = new GetJobResponseBodyPods();
            return TeaModel.build(map, self);
        }

        public GetJobResponseBodyPods setType(String type) {
            this.type = type;
            return this;
        }
        public String getType() {
            return this.type;
        }

        public GetJobResponseBodyPods setPodId(String podId) {
            this.podId = podId;
            return this;
        }
        public String getPodId() {
            return this.podId;
        }

        public GetJobResponseBodyPods setStatus(String status) {
            this.status = status;
            return this;
        }
        public String getStatus() {
            return this.status;
        }

        public GetJobResponseBodyPods setIp(String ip) {
            this.ip = ip;
            return this;
        }
        public String getIp() {
            return this.ip;
        }

        public GetJobResponseBodyPods setGmtGreateTime(String gmtGreateTime) {
            this.gmtGreateTime = gmtGreateTime;
            return this;
        }
        public String getGmtGreateTime() {
            return this.gmtGreateTime;
        }

        public GetJobResponseBodyPods setGmtStartTime(String gmtStartTime) {
            this.gmtStartTime = gmtStartTime;
            return this;
        }
        public String getGmtStartTime() {
            return this.gmtStartTime;
        }

        public GetJobResponseBodyPods setGmtFinishTime(String gmtFinishTime) {
            this.gmtFinishTime = gmtFinishTime;
            return this;
        }
        public String getGmtFinishTime() {
            return this.gmtFinishTime;
        }

    }

}

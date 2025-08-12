// This file is auto-generated, don't edit it. Thanks.
package com.aliyun.openservices.odps.console.xflow;

import com.aliyun.tea.*;

public class JobSpec extends TeaModel {
    // 类型
    @NameInMap("Type")
    public String type;

    // 镜像
    @NameInMap("Image")
    public String image;

    // 实例数量
    @NameInMap("PodCount")
    public Long podCount;

    // Ecs实例规格
    @NameInMap("EcsSpec")
    public String ecsSpec;

    public static JobSpec build(java.util.Map<String, ?> map) throws Exception {
        JobSpec self = new JobSpec();
        return TeaModel.build(map, self);
    }

    public JobSpec setType(String type) {
        this.type = type;
        return this;
    }
    public String getType() {
        return this.type;
    }

    public JobSpec setImage(String image) {
        this.image = image;
        return this;
    }
    public String getImage() {
        return this.image;
    }

    public JobSpec setPodCount(Long podCount) {
        this.podCount = podCount;
        return this;
    }
    public Long getPodCount() {
        return this.podCount;
    }

    public JobSpec setEcsSpec(String ecsSpec) {
        this.ecsSpec = ecsSpec;
        return this;
    }
    public String getEcsSpec() {
        return this.ecsSpec;
    }
}

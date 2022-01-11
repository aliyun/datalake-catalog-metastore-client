package com.aliyun.datalake.metastore.common.entity;

public class StsTokenInfo {
    public String accessKeyId;
    public String accessKeySecret;
    public String stsToken;

    public StsTokenInfo(String accessKeyId, String accessKeySecret, String stsToken) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.stsToken = stsToken;
    }
}

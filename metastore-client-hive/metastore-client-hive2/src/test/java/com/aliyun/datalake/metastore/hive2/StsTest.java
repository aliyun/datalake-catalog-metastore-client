package com.aliyun.datalake.metastore.hive2;

import com.aliyun.datalake.metastore.common.STSHelper;
import org.junit.Test;

public class StsTest {
    @Test
    public void teststsnew() throws Exception {
        try {
            STSHelper.getEMRSTSTokenNew();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void teststsold() throws Exception {
        try {
            STSHelper.getEMRSTSTokenOld();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void teststsemr() throws Exception {
        try {
            STSHelper.getEMRSTSToken("a", "b");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

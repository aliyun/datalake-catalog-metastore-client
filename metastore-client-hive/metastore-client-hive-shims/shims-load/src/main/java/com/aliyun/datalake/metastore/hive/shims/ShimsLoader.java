package com.aliyun.datalake.metastore.hive.shims;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShimsLoader {
    private static final Logger logger = LoggerFactory.getLogger(ShimsLoader.class);
    private static IHiveShims hiveShims;

    public static synchronized IHiveShims getHiveShims() {
        if (hiveShims == null) {
            hiveShims = loadHiveShims();
        }
        return hiveShims;
    }

    private static IHiveShims loadHiveShims() {
        String hiveVersion = HiveVersionInfo.getShortVersion();
        if (Hive2Shims.supportsVersion(hiveVersion)) {
            try {
                if (!containsUpdateTableStatsFast()) {
                    logger.info("Current version is cdh hive2!");
                    return Hive2CDHShims.class.newInstance();
                }
                return Hive2Shims.class.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("unable to get instance of Hive 1.x shim class");
            }
        } else if (Hive3Shims.supportsVersion(hiveVersion)) {
            try {
                return Hive3Shims.class.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("unable to get instance of Hive 2.x shim class");
            }
        } else {
            throw new RuntimeException("Shim class for Hive version " + hiveVersion + " does not exist");
        }
    }

    @VisibleForTesting
    static synchronized void clearShimClass() {
        hiveShims = null;
    }

    public static boolean containsUpdateTableStatsFast() {
        try {
            MetaStoreUtils.class.getMethod("updateTableStatsFast", Database.class, Table.class,
                    Warehouse.class, boolean.class, boolean.class, EnvironmentContext.class);
        } catch (NoSuchMethodException e) {
            // cdh hive2 should not contain updateTableStatsFast
            return false;
        }
        return true;
    }

}

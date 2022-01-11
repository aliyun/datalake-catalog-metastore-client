package com.aliyun.datalake.metastore.hive.common.utils;

import com.aliyun.datalake.metastore.common.util.ProxyLogUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static com.aliyun.datalake.metastore.hive.shims.IHiveShims.realMessage;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static boolean isExternalTable(Table table) {
        return TableType.EXTERNAL_TABLE.name().equals(table.getTableType()) || isExternalTableSetInParameters(table);
    }

    public static boolean isExternalTableSetInParameters(Table table) {
        if (table.getParameters() == null) {
            return false;
        }
        return Boolean.parseBoolean(table.getParameters().get("EXTERNAL"));
    }

    /**
     * Uses the scheme and authority of the object's current location and the path constructed
     * using the object's new name to construct a path for the object's new location.
     */
    public static Path constructRenamedPath(Path newPath, Path currentPath) {
        URI currentUri = currentPath.toUri();
        return new Path(currentUri.getScheme(), currentUri.getAuthority(), newPath.toUri().getPath());
    }

    public static String lowerCaseConvertPartName(String partName) throws MetaException {
        boolean isFirst = true;
        Map<String, String> partSpec = Warehouse.makeEscSpecFromName(partName);
        String convertedPartName = new String();

        for (Map.Entry<String, String> entry : partSpec.entrySet()) {
            String partColName = entry.getKey();
            String partColVal = entry.getValue();

            if (!isFirst) {
                convertedPartName += "/";
            } else {
                isFirst = false;
            }
            convertedPartName += partColName.toLowerCase() + "=" + partColVal;
        }
        return convertedPartName;
    }

    public static boolean supportRewrite() {
        return HiveVersionInfo.getShortVersion().startsWith("2.3") || HiveVersionInfo.getShortVersion().startsWith("3.");
    }

    public static boolean tryRename(FileSystem fileSystem, Path src, Path dest, boolean enableFsOperation) {
        long startTime = System.currentTimeMillis();
        boolean exists = false;
        boolean rename = false;
        try {
            if (enableFsOperation) {
                exists = fileSystem.exists(src);
                rename = fileSystem.rename(src, dest);
                return exists && rename;
            } else {
                exists = true;
                rename = true;
            }

            return exists && rename;

        } catch (IOException ignored) {
            return false;
        } finally {
            final boolean existsFinal = exists;
            final boolean renameFinal = rename;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.tryRename, exists:{}, rename:{}, cost:{}ms, src:{}, dest:{}",
                                                     realMessage(enableFsOperation), existsFinal, renameFinal, System.currentTimeMillis() - startTime, src, dest));
        }
    }

    public static boolean renameDir(Warehouse warehouse, Path src, Path dest, boolean inheritPerms,
                                    boolean enableFsOperation) throws MetaException {
        long startTime = System.currentTimeMillis();
        boolean rename = false;
        try {
            if (enableFsOperation) {
                rename = warehouse.renameDir(src, dest, inheritPerms);
            } else {
                rename = true;
            }
            return rename;
        } finally {
            final boolean renameFinal = rename;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.renameDir, result:{}, cost:{}ms, src:{}, dest:{}",
                                                     realMessage(enableFsOperation), renameFinal, System.currentTimeMillis() - startTime, src, dest));
        }
    }

    public static boolean renameFs(FileSystem fs, Path src, Path dest, boolean enableFsOperation) throws IOException {
        long startTime = System.currentTimeMillis();
        boolean rename = false;
        try {
            if (enableFsOperation) {
                rename = fs.rename(src, dest);
            } else {
                rename = true;
            }

            return rename;
        } finally {
            final boolean renameFinal = rename;
            ProxyLogUtils.printLog(() -> logger.info("dlf.fs.{}.renameFs, result:{}, cost:{}ms, src:{}, dest:{}",
                                                     realMessage(enableFsOperation), renameFinal, System.currentTimeMillis() - startTime, src, dest));
        }
    }

    public static boolean isSubdirectory(Path parent, Path other) {
        return other.toString().startsWith(parent.toString());
    }

    public static boolean isEmptyDir(Warehouse warehouse, Path path) throws IOException, MetaException {
        int listCount = warehouse.getFs(path).listStatus(path).length;
        if (listCount == 0) {
            return true;
        }
        return false;
    }
}

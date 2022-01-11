package com.aliyun.datalake.metastore.hive.common.utils;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaHookWrapper implements HiveMetaHook {
    private final HiveMetaHook hook;

    public HiveMetaHookWrapper(HiveMetaHookLoader hookLoader, Table table) throws MetaException {
        hook = getHook(hookLoader, table);
    }

    private HiveMetaHook getHook(HiveMetaHookLoader hookLoader, Table table) throws MetaException {
        if (hookLoader == null) {
            return null;
        }
        return hookLoader.getHook(table);
    }

    public void preCreateTable(Table table) throws MetaException {
        if (hook != null) {
            hook.preCreateTable(table);
        }
    }

    public void rollbackCreateTable(Table table) throws MetaException {
        if (hook != null) {
            hook.rollbackCreateTable(table);
        }
    }

    public void commitCreateTable(Table table) throws MetaException {
        if (hook != null) {
            hook.commitCreateTable(table);
        }
    }

    public void preDropTable(Table table) throws MetaException {
        if (hook != null) {
            hook.preDropTable(table);
        }
    }

    public void rollbackDropTable(Table table) throws MetaException {
        if (hook != null) {
            hook.rollbackDropTable(table);
        }
    }

    public void commitDropTable(Table table, boolean deleteData) throws MetaException {
        if (hook != null) {
            hook.commitDropTable(table, deleteData);
        }
    }
}

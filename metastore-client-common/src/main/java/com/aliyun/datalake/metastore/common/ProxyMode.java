package com.aliyun.datalake.metastore.common;

public enum ProxyMode {
    METASTORE_ONLY, // read/write Metastore Only
    METASTORE_DLF_FAILURE, // read/write Metastore first and write dlf allow failure
    METASTORE_DLF_SUCCESS, // read/write metastore first and write dlf must success
    DLF_METASTORE_SUCCESS, // read/write dlf first and write metastore must success
    DLF_METASTORE_FAILURE, // read/write dlf first and write metastore allow failure
    DLF_ONLY, // read/write dlf only
}
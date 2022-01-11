English | [简体中文](README-CN.md)
![](https://aliyunsdk-pages.alicdn.com/icons/AlibabaCloud.svg)

# Alibaba Cloud Data Lake Formation(DLF)
Alibaba Cloud Data Lake Formation (DLF) provides fully managed, free of maintenance, high availability, high performance, and scalable metadata services, which can be used to as an external Hive Metastore.

This project datalake-catalog-metastore-client provides the ability to connect DLF metadata. The project is compatible with the Apache Hive Metastore Client interface.

Therefore, engines using the Apache Hive Metastore Client interface (such as Hive/SPARK/TRINO, etc.) can be easily migrated to DLF with the help of this project.

For more information, click [DLF](https://dlf.console.aliyun.com/cn-hangzhou/home)

# How to Use
## Preparing
Firstly, you should open DLF service by clicking [DLF](https://dlf.console.aliyun.com/cn-hangzhou/home)

For more information, click [DLF HELP DOCS](https://help.aliyun.com/document_detail/183496.html)

## 1. Using DLF by Hive

### 1.1 Preparing

hive's version >= 2.1
and it's applied with this [patch](https://issues.apache.org/jira/secure/attachment/12958418/HIVE-12679.branch-2.3.patch) by [jira](https://issues.apache.org/jira/browse/HIVE-12679) 
if you use spark, it's necessary to put the patched hive-exec to the path ${spark_home_dir}/jars


### 1.2. Configuring
hive-site.xml adds the following properties
```
<!--use dlf client-->
<property>
    <name>hive.imetastoreclient.factory.class</name>
    <value>com.aliyun.datalake.metastore.hive2.DlfMetaStoreClientFactory</value>
 </property>
 
 <!--aliyun accessKeyId who has permission to access dlf--> 
<property>
    <name>dlf.catalog.accessKeyId</name>
    <value>*</value> 
</property>

 <!--aliyun accessKeySecret who has permission to access dlf--> 
<property>
    <name>dlf.catalog.accessKeySecret</name>
    <value>*</value>
</property>
 
<!--dlf service'endpoint，for more information：(https://help.aliyun.com/document_detail/197608.html)-->
<property>
    <name>dlf.catalog.endpoint</name>
    <value>datalake.cn-hangzhou.aliyuncs.com</value>
</property>

```
### 1.3.1. Installing DLF Client Sdk (Hive2)
way 1, building by yourself
 > maven command ：mvn clean install -Pdist -DskipTests
 >
 > After building， firstly go to dir matastore-client-assembly/target/ and unzip *.tar.gz, then copy hive2 dir to HIVE_HOME/lib/, now you can DLF.

way 2, using maven dependency

```
<dependency>
  <groupId>com.aliyun.datalake</groupId>
  <artifactId>metastore-client-hive2</artifactId>
  <version>0.2.14</version>
</dependency>
```

### 1.3.2. Installing DLF Client Sdk (Hive3)
way 1,  building by yourself(requirements jdk>=1.8)
 > maven command ：mvn clean install -Pdist -DskipTests
 >
 > After building， firstly go to dir matastore-client-assembly/target/ and unzip *.tar.gz, then copy hive3 dir to HIVE_HOME/lib/, now you can DLF.

way 2, using maven dependency

```
<dependency>
  <groupId>com.aliyun.datalake</groupId>
  <artifactId>metastore-client-hive3</artifactId>
  <version>0.2.14</version>
</dependency>
```

## 2. using DLF by Spark
### 2.1. Preparing
the same as Hive's prerequisites
### 2.2. Configuring
the same as Hive's configuration
### 2.3. Installing DLF Client Sdk
the same as Hive's installing


## Issues
[Opening an Issue](https://github.com/aliyun/alibabacloud-sdk/issues/new), Issues not conforming to the guidelines may be closed immediately.

## Changelog
Detailed changes for each release are documented in the [release notes](./ChangeLog.txt).

## References
* [Latest Release](https://github.com/aliyun/alibabacloud-sdk/tree/master/java)

## License
[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Copyright (c) 2021 Alibaba Cloud
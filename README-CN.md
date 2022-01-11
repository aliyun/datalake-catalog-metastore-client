English | [简体中文](README-CN.md)
![](https://aliyunsdk-pages.alicdn.com/icons/AlibabaCloud.svg)

# 阿里云数据湖构建(DLF)
阿里云数据湖构建(DLF)提供全托管、免运维、高可用、高性能、可扩展的元数据服务，可以用它作为云上的Hive Metastore。
本项目datalake-catalog-metastore-client提供了访问DLF元数据的能力，该项目兼容了Apache Hive Metastore Client接口。
所以使用Apache Hive Metastore Client接口的引擎（比如Hive/SPARK/TRINO等)，借助该项目可以很便捷地迁移到DLF上。

更多内容点击[数据湖构建(DLF)产品页面](https://dlf.console.aliyun.com/cn-hangzhou/home)

# 如何使用
##  前提
开通DLF元数据，点击[数据湖构建(DLF)产品页面](https://dlf.console.aliyun.com/cn-hangzhou/home)

## 1. hive引擎使用DLF

### 1.1 前提

hive版本需要>= 2.1，且hive需要打上这个[jira](https://issues.apache.org/jira/browse/HIVE-12679) 的 [patch](https://issues.apache.org/jira/secure/attachment/12958418/HIVE-12679.branch-2.3.patch)
如果使用spark，也需要把安装patch后的hive-exec放到${spark_home}/jars中

### 1.2. 配置
hive-site.xml中加上如下配置
```
<!--配置使用访问dlf元数据的 client-->
<property>
    <name>hive.imetastoreclient.factory.class</name>
    <value>com.aliyun.datalake.metastore.hive2.DlfMetaStoreClientFactory</value>
 </property>
 
 <!--配置开通dlf的阿里云云账号accessKeyId--> 
<property>
    <name>dlf.catalog.accessKeyId</name>
    <value>*</value> 
</property>

 <!--配置开通dlf的阿里云云账号accessKeySecret--> 
<property>
    <name>dlf.catalog.accessKeySecret</name>
    <value>*</value>
</property>
 
<!--配置dlf服务的endpoint，详见：(https://help.aliyun.com/document_detail/197608.html)-->
<property>
    <name>dlf.catalog.endpoint</name>
    <value>datalake-pre.cn-hangzhou.aliyuncs.com</value>
</property>

```
### 1.3.1. hive2 安装DLF Client sdk
方式1.  自己打包
> 打包命令：mvn clean install -Pdist -DskipTests
>
> 构建完成之后，在matastore-client-assembly/target/就生成一个tar.gz，解压后，拷贝hive2到HIVE_HOME/lib目录下，就可以使用DLF。


方式2. 直接使用maven依赖。

```
<dependency>
  <groupId>com.aliyun.datalake</groupId>
  <artifactId>metastore-client-hive2</artifactId>
  <version>0.2.14</version>
</dependency>
```

### 1.3.2.  hive3 安装DLF Client SDK
方式1.  自己打包
> 打包命令：mvn clean install -Pdist -DskipTests
>
> 构建完成之后，在matastore-client-assembly/target/就生成一个tar.gz，解压后，拷贝hive3到HIVE_HOME/lib目录下，就可以使用DLF。
>
方式2. maven 依赖
```
<dependency>
  <groupId>com.aliyun.datalake</groupId>
  <artifactId>metastore-client-hive3</artifactId>
  <version>0.2.14</version>
</dependency>
```

## 2. spark引擎
### 2.1. 前提
spark依赖的hive版本要求，同hive引擎使用DLF中的【前提】
### 2.2. 配置
spark配置，同hive引擎使用DLF中的配置中的【配置】
### 2.3. 安装DLF Client SDK
spark依赖，同hive引擎使用DLF中的配置中的【依赖】


## Issues
[Opening an Issue](https://github.com/aliyun/alibabacloud-sdk/issues/new), Issues not conforming to the guidelines may be closed immediately.

## Changelog
Detailed changes for each release are documented in the [release notes](./ChangeLog.txt).

## References
* [Latest Release](https://github.com/aliyun/alibabacloud-sdk/tree/master/java)

## License
[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Copyright (c) 2021 Alibaba Cloud
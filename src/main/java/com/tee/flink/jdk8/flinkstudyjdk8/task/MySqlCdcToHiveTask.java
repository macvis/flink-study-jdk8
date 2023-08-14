package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.data.RowData;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
public class MySqlCdcToHiveTask {

    public static void main(String[] args) {
        new MySqlCdcToHiveTask().trigger();
    }

    public void trigger(){
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 配置 Hive 连接
        String hiveCatalogName = "default_catalog";  // Hive Catalog 名称
        String hiveDatabaseName = "demo_schema";  // Hive 数据库名称
        String hiveConfDir = "/Users/Tee/Downloads/aliyun";  // Hive 配置目录路径

        HiveCatalog hiveCatalog = new HiveCatalog(hiveCatalogName, hiveDatabaseName, hiveConfDir);
        tEnv.registerCatalog(hiveCatalogName, hiveCatalog);
        tEnv.useCatalog(hiveCatalogName);

        // MySQL 连接配置
        String hostname = "localhost";  // MySQL 主机名
        int port = 3306;  // MySQL 端口号
        String username = "root";  // MySQL 用户名
        String password = "1234567890";  // MySQL 密码
        String databaseName = "flinkdemo";  // MySQL 数据库名称
        String tableName = "demo";  // MySQL 表名称

        // 创建 MySQLDebeziumSource 同步器
        MySqlSource source = MySqlSource.builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName)
                .startupOptions(StartupOptions.initial())
                .build();

        // 从 MySQL CDC 读取数据流
        DataStream<RowData> dataStream = env.addSource(source)
                .map(debeziumRecord -> {
                    // 在这里根据 CDC 记录解析并返回 Flink 的 RowData 对象
                    return null;
                });

        // 将数据流转换为表
        Table table = tEnv.fromDataStream(dataStream);

        // 注册表
        tEnv.createTemporaryView("<table-name>", table);

        // 将表数据写入 Hive 表
        String hiveTableName = "<hive-table-name>";
        String hiveSinkDDL = "CREATE TABLE IF NOT EXISTS " + hiveDatabaseName + "." + hiveTableName +
                " (col1 STRING, col2 INT, ..., colN DECIMAL) " +
                " STORED AS PARQUET ";

        tEnv.executeSql(hiveSinkDDL);

        TableResult tableResult = tEnv.executeSql("INSERT INTO " + hiveDatabaseName + "." + hiveTableName +
                " SELECT * FROM <table-name>");

        // 执行作业
        env.execute();
    }
    }
}

package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
@Slf4j
public class MySqlCdcToHiveTask {

    public static void main(String[] args) {
        new MySqlCdcToHiveTask().trigger();
    }

    public void trigger(){
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        System.setProperty("HADOOP_USER_NAME", "root");

//        tEnv.getConfig().getConfiguration().setBoolean("hive.metastore.execute.setugi", false);
//        tEnv.getConfig().getConfiguration().setString("hive.metastore.execute.user", "root");

        // 配置 Hive 连接
        // Hive Catalog 名称
        String hiveCatalogName = "default";
        // Hive 数据库名称
        String hiveDatabaseName = "demo_schema";
        // Hive 配置目录路径
        String hiveConfDir = "/Users/Tee/FPI-Tech/bidata_study/xiehong-aliyun";

        HiveCatalog hiveCatalog = new HiveCatalog(hiveCatalogName, hiveDatabaseName, hiveConfDir);
        tEnv.registerCatalog(hiveCatalogName, hiveCatalog);
        tEnv.useCatalog(hiveCatalogName);

        // MySQL 连接配置
        String hostname = "localhost";
        int port = 3306;
        String username = "root";
        String password = "1234567890";
        String databaseName = "flinkdemo";
        String tableName = "demo";

        // 创建 MySQLDebeziumSource 同步器
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .databaseList(databaseName)
                .tableList(databaseName + "." + tableName)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStream<String> dataStream =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // 将数据流转换为表
        Table table = tEnv.fromDataStream(dataStream);

        // 注册表
        tEnv.createTemporaryView("demo_cdc", table);

        // 将表数据写入 Hive 表
        String hiveTableName = "cdc_demo";
        String hiveSinkDDL = "CREATE TABLE IF NOT EXISTS " + hiveDatabaseName + "." + hiveTableName +
                " (id INT, actor STRING, alias STRING) " +
                " STORED AS PARQUET ";

        tEnv.executeSql(hiveSinkDDL);

        TableResult tableResult = tEnv.executeSql("INSERT INTO " + hiveDatabaseName + "." + hiveTableName +
                " SELECT * FROM demo_cdc");

        try{
            // 执行作业
            env.execute();
        }catch(Exception e){
            log.error("", e);
        }
    }
}

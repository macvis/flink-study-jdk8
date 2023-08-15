package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.util.Properties;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
@Slf4j
public class MySqlCdcToHiveBySQL {
    public static void main(String[] args) {
//获取执行环节
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发
        env.setParallelism(6);
        //设置checkpoint
        env.enableCheckpointing(60000);

        env.getConfig().setAutoWatermarkInterval(200);
        // 设置Flink SQL环境
        EnvironmentSettings tableEnvSettings =
                EnvironmentSettings.inStreamingMode();
        // 创建table Env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                tableEnvSettings);
        // 设置checkpoint 模型

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint间隔

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofMinutes(1));
        // 指定catalog名称
        String catalogName = "devHive";
        // 创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(
                catalogName,
                "default",
                "/Users/Tee/Downloads/aliyun"
        );
        //注册 Hive Catalog
        tableEnv.registerCatalog(catalogName, hiveCatalog);
        //使用hive Catalog
        tableEnv.useCatalog(catalogName);
        //创建mysql cdc 数据源
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS cdc");
        // 创建mysql cdc 数据表
        tableEnv.executeSql("DROP TABLE IF EXISTS cdc.demo");
        tableEnv.executeSql("CREATE TABLE cdc.demo(\n" +
                "    id INT PRIMARY KEY NOT ENFORCED,\n" +
                "    actor STRING,\n" +
                "    alias STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '1234567890',\n" +
                "  'database-name' = 'flinkdemo',\n" +
                "  'table-name' = 'demo_for_hive'\n" +
                ")");

        // 创建kafka source
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.order_info");
        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
                "  id INT PRIMARY KEY NOT ENFORCED,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'flink-cdc-topic',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'format' = 'changelog-json'\n" +
                ")");

        // 向kafka表中插入数据
        tableEnv.executeSql("INSERT INTO kafka.demo\n" +
                "SELECT id, actor, alias\n" +
                "FROM cdc.demo");


        // 自定义带op字段的stream
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaConfig.setProperty("group.id", "flink-cdc-group");
        kafkaConfig.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumerBase<String> consumer = new FlinkKafkaConsumer<>(
                "flink-cdc-topic",
                new SimpleStringSchema(),
                kafkaConfig
        ).setStartFromEarliest();
        DataStreamSource<String> streamSource = env.addSource(consumer);


        String[] fieldNames =
                {"id", "actor", "alias"};

        TypeInformation[] types =
                {Types.INT, Types.STRING, Types.STRING};

        SingleOutputStreamOperator<Row> ds2 = streamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                JSONObject json = JSON.parseObject(value);
                int arity = fieldNames.length;
                Row row = new Row(arity);
                row.setField(0, json.getInteger("id"));
                row.setField(1, json.getString("actor"));
                row.setField(2, json.getString("alias"));
                return row;
            }
        }, new RowTypeInfo(types, fieldNames));
        // 设置水印
        tableEnv.createTemporaryView("merged_demo", ds2);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.demo");
        tableEnv.executeSql("CREATE TABLE ods.demo (\n" +
                "  id INT,\n" +
                "   actor STRING,\n" +
                "   alias STRING,\n" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES(\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt$hr:00:00 ',\n" +
                "  'sink.partition-commit.trigger'='partition-time',\n" +
                "  'sink.partition-commit.delay'='1 min',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n " +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO ods.demo\n" +
                "SELECT \n" +
                "id,\n" +
                "actor,\n" +
                "alias,\n" +
                "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm: ss '),' yyyy - MM - dd ') as dt,\n" +
                "DATE_FORMAT(TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss '),' HH ') as hr\n" +
                "FROM merged_demo"
        );
    }
}

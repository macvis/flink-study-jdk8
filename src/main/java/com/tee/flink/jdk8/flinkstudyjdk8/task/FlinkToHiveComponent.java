package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

/**
 * @author youchao.wen
 * @date 2023/8/7.
 */
@Slf4j
@Component
public class FlinkToHiveComponent {

    public static void main(String[] args) {
        new FlinkToHiveComponent().trigger();
    }

    public void trigger() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String catalogName = "hive_catalog";
        HiveCatalog catalog = new HiveCatalog(
                catalogName,
                "default",
                "/usr/local/Cellar/hive/3.1.3/libexec/conf"
        );
        catalog.open();

        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS cdc");
        tableEnv.executeSql("DROP TABLE IF EXISTS cdc.demo");
        tableEnv.executeSql("CREATE TABLE cdc.demo(\n" +
                "    id INT,\n" +
                "    actor STRING,\n" +
                "    alias TIMESTAMP\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '1234567890',\n" +
                "  'database-name' = 'flinkdemo',\n" +
                "  'table-name' = 'demo'\n" +
                ")");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.demo");
        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
                "  id INT,\n" +
                "  actor STRING,\n" +
                "  alias TIMESTAMP\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-cdc-topic',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'format' = 'changelog-json'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO kafka.demo \n" +
                "SELECT id, actor, alias \n" +
                "FROM cdc.demo");

        // 定义带op字段的stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-cdc-group");

        FlinkKafkaConsumerBase<String> consumer = new FlinkKafkaConsumer<>(
                "demo",
                new SimpleStringSchema(),
                properties
        ).setStartFromEarliest();

        DataStream<String> ds = env.addSource(consumer);

        String[] fieldNames = {"id", "actor", "alias"};
        TypeInformation[] types = {Types.LONG, Types.STRING, Types.STRING};
        DataStream<Row> ds2 = ds.map(str -> {
            JSONObject jsonObject = JSON.parseObject(str);
            JSONObject data = jsonObject.getJSONObject("data");
            int arity = fieldNames.length;
            Row row = new Row(arity);
            row.setField(0, data.get("id"));
            row.setField(1, data.get("actor"));
            row.setField(2, data.get("alias"));

            return row;
        }, new RowTypeInfo(types, fieldNames));

        tableEnv.registerDataStream("merged_demo", ds2);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.demo");

        tableEnv.executeSql("CREATE TABLE ods.demo (\n" +
                "  id INT,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
                ") PARTITIONED BY (\n" +
                "    ts_date STRING,\n" +
                "    ts_hour STRING,\n" +
                "    ts_minute STRING\n" +
                ") STORED AS PARQUET TBLPROPERTIES (\n" +
                "  'sink.partition-commit.trigger' = 'partition-time',\n" +
                "  'sink.partition-commit.delay' = '1 min',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file',\n " +
                "  'partition.time-extractor.timestamp-pattern' = '$ts_date$ts_hour:$ts_minute:00 '\n" +
        ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        try{
            tableEnv.executeSql("INSERT INTO ods.demo \n" +
                    "SELECT id, actor, alias,\n" +
                    " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' yyyyMMdd ') as ts_date, \n" +
                    " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' HH ') as ts_hour, \n" +
                    " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' mm ') as ts_minute \n" +
                    " FROM merged_demo");
            env.execute("");
        }catch(Exception e){
            log.error("", e);
        }

    }

    private static String getOperation(String op) {
        String operation = "INSERT";
        for (RowKind rk : RowKind.values()) {
            if (rk.shortString().equals(op)) {
                switch (rk) {
                    case UPDATE_BEFORE:
                    case UPDATE_AFTER:
                        operation = "UPDATE";
                        break;
                    case DELETE:
                        operation = "DELETE";
                        break;
                    case INSERT:
                    default:
                        operation = "INSERT";
                        break;
                }
                break;
            }
        }
        return operation;
    }
}

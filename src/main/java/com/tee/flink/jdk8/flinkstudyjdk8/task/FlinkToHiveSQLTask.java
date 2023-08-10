package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.tee.flink.jdk8.flinkstudyjdk8.entity.Demo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * hive 不支持带有 update delete 的数据写入，不支持
 *
 *  这个 task 作废，代码保留
 *
 *
 *
 * @author youchao.wen
 * @date 2023/8/7.
 */
@Slf4j
@Component
public class FlinkToHiveSQLTask {

    public static void main(String[] args) {
        new FlinkToHiveSQLTask().trigger();

    }

    public void trigger() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

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

        Table cdcData = tableEnv.sqlQuery("select * from cdc.demo");
        DataStream<Row> cdcDataStream = tableEnv.toChangelogStream(cdcData);
        log.info("=========cdcData========");
        cdcDataStream.print();
        log.info("===========");


        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.demo");
        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
                "  id INT PRIMARY KEY NOT ENFORCED,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'flink-cdc-topic',\n" +
                "  'properties.group.id' = 'flink-cdc-group', \n" +
                "  'properties.auto.offset.reset' = 'earliest',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'\n" +
                ")");

//        String insertSql = "INSERT INTO kafka.demo SELECT * FROM cdc.demo";
//        TableResult insertToKafkaResult = tableEnv.executeSql(insertSql);
//        log.info("======= {} =========", insertSql);
//        insertToKafkaResult.print();


//        Table kafkaData = tableEnv.sqlQuery("select * from kafka.demo");
//        DataStream<Row> kafkaDatStream = tableEnv.toChangelogStream(kafkaData);
//        log.info("=========kafkaData========");
//        kafkaDatStream.print();
//        log.info("=================");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-cdc-group");
        properties.setProperty("auto.offset.reset", "earliest");

//        Schema schema = Schema.newBuilder()
//                .column("id", DataTypes.INT())
//                .column("actor", DataTypes.STRING())
//                .column("alias", DataTypes.STRING())
//                .build();

        FlinkKafkaConsumerBase<Demo> consumer = new FlinkKafkaConsumer<>(
                "flink-cdc-topic",
                new JsonDeserializationSchema<>(Demo.class),
                properties
        ).setStartFromEarliest();


        DataStream<Demo> ds = env.addSource(consumer);

//        DataStream<Demo> ds2 = ds.map(str -> {
//            JSONObject jsonObject = JSON.parseObject(str);
//            JSONObject data = jsonObject.getJSONObject("data");
//            Demo row = new Demo();
//            row.setId(data.getLong("id"));
//            row.setActor(data.getString("actor"));
//            row.setAlias(data.getString("alias"));
//
//            return row;
//        });

//        tableEnv.registerDataStream("merged_demo", ds);

//        Schema schema = Schema.newBuilder()
//                .column("id", DataTypes.INT())
//                .column("actor", DataTypes.STRING())
//                .column("alias", DataTypes.STRING())
//                .build();

//        tableEnv.createTemporaryView("merged_demo", ds);

//        System.out.println("existed table kafka.merged_demo =======");
//        tableEnv.executeSql("existed table kafka.merged_demo").print();

//        System.out.println("exists table merged_demo =======");
//        tableEnv.executeSql("select * from merged_demo").print();



//        Table tempData = tableEnv.sqlQuery("select * from kafka.merged_demo");
//        DataStream<Row> tempDataStream = tableEnv.toChangelogStream(tempData);
//        log.info("=========tempData========");
//        tempDataStream.print();
//        log.info("=================");


        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.demo");

        tableEnv.executeSql("CREATE TABLE ods.demo (\n" +
                "  id BIGINT,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
                ") " +
//                "PARTITIONED BY (\n" +
//                "    ts_date STRING,\n" +
//                "    ts_hour STRING,\n" +
//                "    ts_minute STRING\n" +
//                ") " +
                "STORED AS PARQUET TBLPROPERTIES (\n" +
                "  'sink.partition-commit.trigger' = 'partition-time',\n" +
                "  'sink.partition-commit.delay' = '1 min',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n " +
//                "  , 'partition.time-extractor.timestamp-pattern' = '$ts_date$ts_hour:$ts_minute:00 '\n" +
        ")");

        tableEnv.useDatabase("ods");

//        Table table = tableEnv.fromDataStream(ds);
//        table.executeInsert("demo");
//
//        tableEnv.createTemporaryView("merged_demo", tableEnv.from("demo"));
//
//        Table result = tableEnv.sqlQuery("SELECT * from merged_demo");
//        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(result);
//        rowDataStream.print();
//
//        DataStream<Demo> writerStream = tableEnv.toDataStream(result, Demo.class);
//        writerStream.print();

        try{
            tableEnv.executeSql("INSERT INTO ods.demo \n" +
                    "SELECT id, actor, alias " +
//                    ", DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' yyyyMMdd ') as ts_date, \n" +
//                    " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' HH ') as ts_hour, \n" +
//                    " DATE_FORMAT(TO_TIMESTAMP(create_time, 'yyyy-MM-dd HH:mm:ss '), ' mm ') as ts_minute \n" +
                    " FROM kafka.demo");
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

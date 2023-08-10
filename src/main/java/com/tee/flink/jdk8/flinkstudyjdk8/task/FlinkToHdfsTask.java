package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;


/**
 * @author youchao.wen
 * @date 2023/8/10.
 */
@Slf4j
@Component
public class FlinkToHdfsTask {

    public static void main(String[] args) {
        new FlinkToHdfsTask().trigger();
    }

    public void trigger(){
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

        Table kafkaTable = tableEnv.sqlQuery("SELECT * FROM kafka.demo");

        DataStream<Row> kafkaDataStream = tableEnv.toChangelogStream(kafkaTable);
        log.info("======= kafkaDataStream =======");
        kafkaDataStream.print();

//        String hdfsPath = "hdfs://localhost:9000/flink/demo";
        String hdfsPath = "/flink/demo";
        StreamingFileSink<Tuple3<Integer, String, String>> sink = StreamingFileSink
                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<Tuple3<Integer, String, String>>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(3))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();


        kafkaDataStream.map(row -> new Tuple3<>((Integer) row.getField(0), (String)row.getField(1), (String)row.getField(2)))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Integer, String, String>>() {}))
                .addSink(sink);

        try{
            env.execute();
        }catch(Exception e){
            log.error("", e);
        }
    }
}

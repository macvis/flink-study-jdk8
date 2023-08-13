package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF;
import org.springframework.stereotype.Component;


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

    public void trigger() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);


        System.setProperty("HADOOP_USER_NAME", "root");
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


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
//                "  id INT ,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
//                "  , metadata ROW<timestamp BIGINT, offset BIGINT>\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
//                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-cdc-topic',\n" +
                "  'properties.group.id' = 'flink-cdc-group', \n" +
                "  'properties.auto.offset.reset' = 'latest',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'format' = 'json'" +
                "   'key.format' = 'json'," +
                "  'value.format' = 'json'\n" +
                ")");
        tableEnv.executeSql("INSERT INTO kafka.demo SELECT * FROM cdc.demo");


        Table kafkaTable = tableEnv.sqlQuery("SELECT * FROM kafka.demo");
        DataStream<Row> kafkaDataStream = tableEnv.toChangelogStream(kafkaTable);
        log.info("======= kafkaDataStream =======");
        kafkaDataStream.print();


        String hdfs = "hdfs://47.243.131.115:8020/user/hive/warehouse/hdfs/demo.csv";
        Path hdfsPath = new Path(hdfs);

        String localcsv = "/Users/Tee/Downloads/hive/demo.csv";

        CsvOutputFormat<Tuple3<Integer, String, String>> csvOutputFormat = new CsvOutputFormat<>(
                hdfsPath, "\n",","
        );
        csvOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        kafkaDataStream.map(row -> row.getField(0) + ","
                                + row.getField(1) + ","
                                + row.getField(2))
                .returns(Types.STRING())
                .writeAsText(localcsv, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("Write to HDFS");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}

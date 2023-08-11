package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
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

//        String catalogName = "hdfswriter";
//        HiveCatalog catalog = new HiveCatalog(
//                catalogName,
//                "default",
//                "/usr/local/Cellar/hive/3.1.3/libexec/conf"
//        );
//        catalog.open();
//
//        tableEnv.registerCatalog(catalogName, catalog);
//        tableEnv.useCatalog(catalogName);

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
//        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
////                "  id INT PRIMARY KEY NOT ENFORCED,\n" +
//                "  id INT ,\n" +
//                "  actor STRING,\n" +
//                "  alias STRING\n" +
////                "  , metadata ROW<timestamp BIGINT, offset BIGINT>\n" +
//                ") WITH (\n" +
////                "  'connector' = 'upsert-kafka',\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'flink-cdc-topic',\n" +
//                "  'properties.group.id' = 'flink-cdc-group', \n" +
//                "  'properties.auto.offset.reset' = 'latest',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'format' = 'json'" +
////                "  , 'key.format' = 'json'," +
////                "  'value.format' = 'json'\n" +
//                ")");

        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
                "  `id` INT,\n" +
                "  `actor` STRING,\n" +
                "  `alias` STRING\n" +
//                "  `,ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'\n" +
//                "  , `watermark` for pt as pt - interval '2' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-cdc-topic',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'flink-cdc-group',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'debezium-json'\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.hdfs_sink");
//        tableEnv.executeSql("CREATE TABLE kafka.hdfs_sink (\n" +
//                "    id INT,\n" +
//                "    actor STRING,\n" +
//                "    alias STRING\n" +
//                ") WITH (\n" +
//                "    'connector' = 'filesystem',\n" +
//                "    'path' = 'hdfs://localhost:9000/flink/demo',\n" +
//                "    'format' = 'csv'\n" +
//                ")");

//        tableEnv.executeSql("CREATE VIEW kafka.kafka_demo_view AS SELECT * FROM kafka.demo");
//        tableEnv.executeSql("INSERT INTO kafka.hdfs_sink SELECT * FROM kafka.kafka_demo_view");


        Table kafkaTable = tableEnv.sqlQuery("SELECT * FROM kafka.demo");
        DataStream<Row> kafkaDataStream = tableEnv.toChangelogStream(kafkaTable);
        log.info("======= kafkaDataStream =======");
//        kafkaDataStream.print();
        DataStream<String> kafkaString = kafkaDataStream.map(row ->
                row.getField(0) + "," + row.getField(1) + "," + row.getField(2)
        ).returns(Types.STRING());
        kafkaString.print();

//
//        tableEnv.executeSql("INSERT INTO kafka.demo SELECT * FROM cdc.demo");
//
//        String hdfsPath = "hdfs://localhost:9000/flink/demo";
//        String hdfsPath = "hdfs://47.111.76.127:9000/user/hive";
//        String hdfsPath = "hdfs://47.243.131.115:9000/user/hive/warehouse/hdfswriter.db";
//        FileSink<String> sink = FileSink
//                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                // 自定义分桶策略
//                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
//                .build();
//        kafkaString.sinkTo(sink);


//        StreamingFileSink<String> hdfsSink = StreamingFileSink
//                // 输出的文件是按行存储的
//                .forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>("UTF-8"))
//                // 也可以将输出结果用 Parquet 等格式进行压缩存储
////                .forBulkFormat(new Path(hdfsPath))
//                //分桶策略 默认"yyyy-MM-dd--HH"  这里设置按天分
//                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
//                //分桶策略: 不分桶，所有文件写到根目录；
////                .withBucketAssigner(new BasePathBucketAssigner())
//                .withRollingPolicy(rollingPolicy)
//                .withBucketCheckInterval(1000L) // 桶检查间隔，这里设置为1s
//                .build();
//
//        // 添加Source、Sink
//        DataStreamSource<String> sourceStream = env.addSource(kafkaString);
//        sourceStream.addSink(hdfsSink);

        try{
            env.execute("Write to HDFS");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

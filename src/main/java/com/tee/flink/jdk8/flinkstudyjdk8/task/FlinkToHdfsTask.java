package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.tee.flink.jdk8.flinkstudyjdk8.entity.Demo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.util.Objects;
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

    public void trigger() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hdfsHost = "hdfs://47.97.113.63:8020";


//        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(hdfsHost + "/user/checkpoint");
//        env.setDefaultSavepointDirectory(hdfsHost + "/user/checkpoint");

//        env.getCheckpointConfig().setCheckpointStorage("file:/Users/Tee/Downloads/checkpoint/");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart( 3, TimeUnit.MINUTES.toMillis(1)));
                env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        // 每 ** ms 开始一次 checkpoint
//        env.enableCheckpointing(10*1000);
//        // 设置模式为精确一次 (这是默认值)
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        // 确认 checkpoints 之间的时间会进行 ** ms
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        // 同一时间只允许一个 checkpoint 进行
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 开启在 job 中止后仍然保留的 externalized checkpoints
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);


        env.setParallelism(1);
//        env.enableCheckpointing(120000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(12000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend stateBackend = new FsStateBackend(hdfsHost + "/user/fs_state_backend");
//        StateBackend stateBackend = new FsStateBackend("file:///Users/Tee/Downloads/fs_state_backend");
        env.setStateBackend(stateBackend);



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


       /* tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS kafka");
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
        */

        SingleOutputStreamOperator<Demo> singleOutputStreamOperator =
                cdcDataStream.filter(Objects::nonNull)
                        .flatMap(new FlatMapFunction<Row, Demo>() {
                            @Override
                            public void flatMap(Row row, Collector<Demo> out) throws Exception {
                                Demo demo = new Demo();
                                demo.setId((Integer) row.getField(0));
                                demo.setActor((String) row.getField(1));
                                demo.setAlias((String) row.getField(2));
                                out.collect(demo);
                            }
                });
        System.out.println("======= singleOutputStreamOperator ========");
        singleOutputStreamOperator.print();

        String hdfs = hdfsHost + "/user/hive/warehouse/hdfs";
//        Path hdfsPath = new Path(hdfs);

        String local = "/Users/Tee/Downloads/hive/files";
        Path hdfsPath = new Path(hdfs);

        StreamingFileSink<Demo> sink =
                StreamingFileSink.forRowFormat(hdfsPath,
                                new SimpleStringEncoder<Demo>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .build();
        singleOutputStreamOperator.addSink(sink);


        try {
           JobID jobID = env.execute("Write to HDFS").getJobID();
           log.info("jobID = {}", jobID);
        } catch (Exception e) {
            log.error("", e);
        }
    }
}

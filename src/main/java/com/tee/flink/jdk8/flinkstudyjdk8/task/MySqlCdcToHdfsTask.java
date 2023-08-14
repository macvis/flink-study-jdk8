package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.tee.flink.jdk8.flinkstudyjdk8.entity.Demo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
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
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author youchao.wen
 * @date 2023/8/13.
 */
@Slf4j
@Deprecated
public class MySqlCdcToHdfsTask {

    public static void main(String[] args) {
        new MySqlCdcToHdfsTask().trigger();
    }

    public void trigger(){
        // 设置流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(120000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StateBackend stateBackend =
                new FsStateBackend("hdfs://47.243.131.115:8020/user/fs_state_backend");
        env.setStateBackend(stateBackend);


        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        System.setProperty("HADOOP_USER_NAME", "root");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS cdc");
        tEnv.executeSql("DROP TABLE IF EXISTS cdc.demo");
        tEnv.executeSql("CREATE TABLE cdc.demo(\n" +
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
//                "  , 'debezium-json-schema-include' = 'true'\n" +
                ")");

        // 将数据写入 HDFS
//        String hdfs = "hdfs://47.243.131.115:8020/user/hive/warehouse/hdfs";
//        Path hdfsPath = new Path(hdfs);

        String localPath = "file:///Users/Tee/Downloads/files";

        Table table = tEnv.from("cdc.demo");
        DataStream<Tuple2<Boolean, Row>> stream =
                tEnv.toRetractStream(table, GenericTypeInfo.of(Row.class));


        SingleOutputStreamOperator<Demo> stuSingleOutputStreamOperator =
                stream.filter(Objects::nonNull)
                        .flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Demo>() {
                            @Override
                            public void flatMap(Tuple2<Boolean, Row> data, Collector<Demo> out) throws Exception {
                                Demo demo = new Demo();
                                Row row =  data.f1;
                                demo.setId((Integer) row.getField(0));
                                demo.setActor((String) row.getField(1));
                                demo.setAlias((String) row.getField(2));
                                out.collect(demo);
                            }
                        });

        StreamingFileSink<Demo> build =
                StreamingFileSink.forRowFormat(new Path(localPath),
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

        stuSingleOutputStreamOperator.addSink(build);

        try{
            env.execute();
        }catch(Exception e){
            log.error("", e);
        }

    }
}

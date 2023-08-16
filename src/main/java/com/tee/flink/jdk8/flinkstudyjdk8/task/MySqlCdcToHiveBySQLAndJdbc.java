package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.tee.flink.jdk8.flinkstudyjdk8.pojo.dto.CdcDataJsonDTO;
import com.tee.flink.jdk8.flinkstudyjdk8.processor.KafkaMsgProcessor;
import com.tee.flink.jdk8.flinkstudyjdk8.serde.JsonDeserializationSchema;
import com.tee.flink.jdk8.flinkstudyjdk8.sink.HiveJdbcSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.springframework.stereotype.Component;

import java.util.Properties;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
@Slf4j
@Component
public class MySqlCdcToHiveBySQLAndJdbc {


    public static void main(String[] args) {
        new MySqlCdcToHiveBySQLAndJdbc().trigger();
    }

    public void trigger() {
        //获取执行环节
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty("HADOOP_USER_NAME", "root");

        String hdfsHost = "hdfs://47.243.131.115:8020";

        // 设置并发
        env.setParallelism(1);
        //设置checkpoint
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(hdfsHost + "/user/checkpoint");
        // 设置Flink SQL环境
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.inStreamingMode();
        // 创建table Env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);

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
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka.demo");
        tableEnv.executeSql("CREATE TABLE kafka.demo (\n" +
                "  id INT PRIMARY KEY NOT ENFORCED,\n" +
                "  actor STRING,\n" +
                "  alias STRING\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'flink-cdc-topic',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'format' = 'debezium-json'\n" +
                ")");

        // 向kafka表中插入数据
        tableEnv.executeSql("INSERT INTO kafka.demo\n" +
                "SELECT id, actor, alias\n" +
                "FROM cdc.demo");


        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "localhost:9092");
        kafkaConfig.setProperty("group.id", "flink-cdc-group");
        kafkaConfig.setProperty("auto.offset.reset", "latest");
        kafkaConfig.setProperty("enable.auto.commit", "true");
        kafkaConfig.setProperty("auto.commit.interval.ms", "100");


        FlinkKafkaConsumerBase<CdcDataJsonDTO> kafkaSource = new FlinkKafkaConsumer<>(
                "flink-cdc-topic", new JsonDeserializationSchema(), kafkaConfig)
                // 从最新的消息取的数据，适合增量写入
                .setStartFromLatest()
                // 全量 kafka 消息，适合初次同步或者故障恢复
//                 .setStartFromEarliest()
                .setCommitOffsetsOnCheckpoints(true);

//        KafkaSource<CdcDataJsonDTO> kafkaSource = KafkaSource.<CdcDataJsonDTO>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("flink-cdc-topic")
//                .setDeserializer(new KafkaJsonMsgDeserializer())
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setProperties(kafkaConfig)
//                .build();

        DataStreamSource<CdcDataJsonDTO> streamSource = env.addSource(kafkaSource);
        streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                .keyBy(value -> value)
                .process(new KafkaMsgProcessor())
                .addSink(new HiveJdbcSink().tableName("demo_cdc"));


        try {
            streamSource.print();
            env.execute("MySqlCDC execute");
        } catch (Exception e) {
            log.error("", e);
        }
    }


    private void trigger2() {

        // 指定catalog名称, 默认是这个default_catalog
        String catalogName = "default_catalog";
        // 创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(
                catalogName,
                "demo_schema",
                "/Users/Tee/FPI-Tech/bidata_study/xiehong-aliyun"
        );

        hiveCatalog.open();
        //注册 Hive Catalog，没有新建 catalog 就不需要注册
//        tableEnv.registerCatalog(catalogName, hiveCatalog);
        //使用hive Catalog
//        tableEnv.useCatalog(catalogName);


        //        String[] fieldNames =
//                {"id", "actor", "alias"};
//
//        TypeInformation[] types =
//                {Types.INT, Types.STRING, Types.STRING};
//
//        SingleOutputStreamOperator<Row> ds2 = streamSource.map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String value) throws Exception {
//                JSONObject json = JSON.parseObject(value);
//                int arity = fieldNames.length;
//                Row row = new Row(arity);
//                row.setField(0, json.getInteger("id"));
//                row.setField(1, json.getString("actor"));
//                row.setField(2, json.getString("alias"));
//                return row;
//            }
//        }, new RowTypeInfo(types, fieldNames));
//        tableEnv.createTemporaryView("merged_demo", ds2);

/*        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("DROP TABLE IF EXISTS ods.demo");
        tableEnv.executeSql("CREATE TABLE ods.demo (\n" +
                "  id INT,\n" +
                "   actor STRING,\n" +
                "   alias STRING\n" +
                ")\n"
//                        +
//                "PARTITIONED BY (\n" +
//                "    ts_date STRING,\n" +
//                "    ts_hour STRING,\n" +
//                "    ts_minute STRING\n" +
//                ") STORED AS PARQUET TBLPROPERTIES (\n" +
//                "  'sink.partition-commit.trigger' = 'partition-time',\n" +
//                "  'sink.partition-commit.delay' = '1 min',\n" +
//                "  'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
//                "  'partition.time-extractor.timestamp-pattern' = '$ts_date$ts_hour:$ts_minute:00'\n" +
//                ")\n"
//                "WITH (\n" +
//                        "  'connector' = 'hive',\n" +
//                        "  'warehouse-dir' = '/user/hive/warehouse',\n" +
//                        "  'table-name' = 'demo'\n" +
//                        ")"
        );*/


        // 表的路径
//        ObjectPath tablePath = new ObjectPath("demo_schema", "demo_cdc");
//
//        Map<String, String> hiveProp = new HashMap<>();
//        hiveProp.put("connector", "hive");
//        hiveProp.put("warehouse-dir", "/user/hive/warehouse");
//
//
//        DataType[] fieldTypes = {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()};
//
//        TableSchema tableSchema = TableSchema.builder()
//                .fields(fieldNames, fieldTypes)
//                .build();
//        CatalogTable catalogTable = new CatalogTableImpl(tableSchema, hiveProp, null);


//            hiveCatalog.dropTable(tablePath, true);
//            hiveCatalog.createTable(tablePath, catalogTable, true);


//            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//            tableEnv.executeSql("INSERT INTO demo_schema.demo_cdc \n" +
//                    "SELECT id, actor, alias FROM merged_demo"
//            );


    }
}

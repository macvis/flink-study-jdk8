package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.util.Collector;

/**
 * @author youchao.wen
 * @date 2023/8/8.
 */
@Slf4j
@Deprecated
public class FlinkToHiveBySocketAPI {

    public static void main(String[] args) {
        new FlinkToHiveBySocketAPI().trigger();
    }

    public void trigger() {
        // 设置流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建配置对象
        Configuration flinkConfig = new Configuration();

        // 将配置对象应用到流执行环境
        // 将配置对象应用到全局配置中
        env.getConfig().setGlobalJobParameters(flinkConfig);


        // 创建一个 Hive Catalog
        String catalogName = "hive_catalog";

        HiveCatalog hive = new HiveCatalog(
                catalogName,
                "default",
                "/usr/local/Cellar/hive/3.1.3/libexec/conf"
        );
        hive.open();

        // 创建一个 StreamTableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.registerCatalog(catalogName, hive);
        tableEnv.useCatalog(catalogName);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 定义数据流
//        DataStream<Tuple3<Integer, String, Integer>> inputDataStream =
        SingleOutputStreamOperator<Tuple2<Integer, String>> inputDataStream =
                env.socketTextStream("localhost", 3306)
                        .process(new ProcessFunction<String, Tuple2<Integer, String>>() {
            @Override
            public void processElement(
                    String value,
                    ProcessFunction<String, Tuple2<Integer, String>>.Context ctx,
                    Collector<Tuple2<Integer, String>> out) throws Exception {
                String[] split = value.split(",");
                String s = split[0];

                out.collect(Tuple2.of(Integer.valueOf(s), split[1].toString()));
            }
        });// 你的数据流来源
        inputDataStream.print("---");

        // 将 DataStream 转换为 Table
        Table inputTable = tableEnv.fromDataStream(inputDataStream);

        // 将 Table 注册为 Hive 表
        String hiveTableName = "demo";

        tableEnv.executeSql(" SELECT id, name FROM " + inputTable).print();
        // 将数据插入到 Hive 表中
//        tableEnv.executeSql("INSERT INTO " + "studentlyz01" + " SELECT id, name FROM " + inputTable);
        // 将数据插入到 Hive 表中
//        tableEnv.executeSql("INSERT INTO " + "user_info102" + " SELECT id, CAST(name AS VARCHAR) FROM " + inputTable);

//        tableEnv.executeSql("INSERT OVERWRITE " + hiveTableName + " SELECT userId, username FROM " + inputTable);

        // 执行 Flink 作业
        try{
            env.execute("写入 Hive 示例");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

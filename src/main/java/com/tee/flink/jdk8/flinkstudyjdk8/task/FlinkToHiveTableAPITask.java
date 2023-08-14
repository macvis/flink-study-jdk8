package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.tee.flink.jdk8.flinkstudyjdk8.entity.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;

/**
 * chatgpt写的，无法运行，怀疑AI 自己扯淡写的
 *
 *
 * @author youchao.wen
 * @date 2023/8/9.
 */
@Slf4j
@Component
@Deprecated
public class FlinkToHiveTableAPITask {

    public static void main(String[] args) {
        new FlinkToHiveTableAPITask().trigger();
    }


    public void trigger(){
        // 初始化Streaming解析环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("id", DataTypes.INT())
                .build();

        // 创建流
        DataStream<Person> stream = env.fromElements(
                new Person("Alice", 1),
                new Person("Bob", 2),
                new Person("Charlie", 3)
        );

        // 注册表
        tableEnv.createTemporaryView("source_table", stream, schema);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // 定义 Hive Sink
        tableEnv.executeSql(
                "CREATE TABLE hive_sink (\n" +
                        "  name STRING,\n" +
                        "  id INT\n" +
                        ") STORED AS parquet TBLPROPERTIES (\n" +
                        "'sink.partition-commit.trigger'='partition-time',\n" +
                        "'sink.partition-commit.delay'='1 h',\n" +
                        "'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
                        ")"
        );

        // 写入数据
        tableEnv.executeSql("INSERT INTO hive_sink SELECT * FROM source_table");

        try{
            // 启动作业
            env.execute("Write to Hive Example Job");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

/**
 * @author youchao.wen
 * @date 2023/8/7.
 */
@Slf4j
@Component
public class MySqlCdcReadComponent {

    public static void main(String[] args) throws Exception {
        new MySqlCdcReadComponent().trigger();
    }

    public void trigger(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/Users/Tee/hadoop_db/hdfs/checkpoint");
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //创建cdc连接器，去读mysql中需要读的表
        tenv.executeSql("CREATE TABLE demo(\n" +
                "                    id INT PRIMARY KEY NOT ENFORCED,\n" +
                "                    actor STRING,\n" +
                "                    alias STRING\n" +
                "                ) WITH (\n" +
                "                    'connector' = 'mysql-cdc',\n" +
                "                    'hostname'  = 'localhost',\n" +
                "                    'port' = '3306',\n" +
                "                    'username' = 'root',\n" +
                "                    'password' = '1234567890',\n" +
                "                    'database-name' = 'flinkdemo' ,\n" +
                "                    'table-name'  = 'demo'\n" +
                "                )");

        Table table = tenv.sqlQuery("select * from demo");
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);
        rowDataStream.print();
        try{
            env.execute();
        }catch(Exception e){
            log.error("", e);
        }

    }
}

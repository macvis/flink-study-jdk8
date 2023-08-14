package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

/**
 * @author youchao.wen
 * @date 2023/8/7.
 */
@Slf4j
@Component
@Deprecated
public class MySqlJdbcReadTask {

    public static void main(String[] args) {
        new MySqlJdbcReadTask().trigger();
    }

    public void trigger(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置执行环境
        env.setParallelism(1);

        // 配置 JDBC 连接
        String jdbcUrl = "jdbc:mysql://localhost:3306/flinkdemo";
        String username = "root";
        String password = "1234567890";
        String tableName = "demo";

        // 定义 JdbcInputFormat
        JdbcInputFormat.JdbcInputFormatBuilder jdbcInputFormatBuilder = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl(jdbcUrl)
                .setUsername(username)
                .setPassword(password);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(Types.LONG, Types.STRING, Types.STRING);
        JdbcInputFormat jdbcInputFormat = jdbcInputFormatBuilder.setQuery("SELECT * FROM " + tableName)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStreamSource<Row> resultSet = env.createInput(jdbcInputFormat);
        resultSet.print("+++++++++");

        try{
            env.execute("");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

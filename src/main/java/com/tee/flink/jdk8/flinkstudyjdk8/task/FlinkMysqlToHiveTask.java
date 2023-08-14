package com.tee.flink.jdk8.flinkstudyjdk8.task;

import com.alibaba.fastjson.JSONObject;
import com.tee.flink.jdk8.flinkstudyjdk8.reader.MysqlReader;
import com.tee.flink.jdk8.flinkstudyjdk8.writer.HiveWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author youchao.wen
 * @date 2023/8/14.
 */
@Slf4j
public class FlinkMysqlToHiveTask {

    public static void main(String[] args) {
        new FlinkMysqlToHiveTask().trigger();
    }

    public void trigger(){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<JSONObject> mysqlData = env.addSource(new MysqlReader());
        mysqlData.addSink(new HiveWriter());
        mysqlData.print();
        try{
            env.execute("Mysql2Hive");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

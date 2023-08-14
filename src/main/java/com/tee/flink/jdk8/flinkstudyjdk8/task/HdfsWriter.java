package com.tee.flink.jdk8.flinkstudyjdk8.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

/**
 * @author youchao.wen
 * @date 2023/8/11.
 */
@Slf4j
@Component
public class HdfsWriter {

    public static void main(String[] args) {
        new HdfsWriter().trigger();
    }

    public void trigger(){
        try{
            System.setProperty("HADOOP_USER_NAME", "root");
            String host = "hdfs://47.97.113.63:8020";
            String dest =  host + "/user/hive/warehouse/hdfs/";

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", host);
            FileSystem fs = FileSystem.get(conf);

            Path path = new Path(dest + "data.txt");
            FSDataOutputStream out;

            // 检查文件是否已存在
            if (fs.exists(path)) {
                out = fs.append(path);
            } else {
                out = fs.create(path);
            }

            // 写入一些测试数据
            for (int i = 0; i < 10; i++) {
                out.writeBytes(i + "\t" + "name" + i + "\n");
            }
            out.close();
            fs.close();
            log.info("hdfs write done");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

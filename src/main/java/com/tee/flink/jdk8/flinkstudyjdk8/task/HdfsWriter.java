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
            //配置HDFS文件系统的地址，替换为你的Hadoop集群地址
//            String dest = "hdfs://47.111.76.127:9000/user/hive/warehouse/demo/";
            String host = "hdfs://47.243.131.115:8020";
            String dest =  host + "/user/hive/warehouse/hdfswriter.db/";

            Configuration conf = new Configuration();
            // 设置HDFS地址
            conf.set("fs.defaultFS", host);
            // 通过FileSystem类的get()方法获取FileSystem对象
            FileSystem fs = FileSystem.get(conf);

            Path path = new Path(dest + "data.txt");
            FSDataOutputStream out;

            // 检查文件是否已存在
            if (fs.exists(path)) {
                System.out.println("File already exists. Appending to it.");
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

            System.out.println("Data written to HDFS successfully.");
        }catch(Exception e){
            log.error("", e);
        }
    }
}

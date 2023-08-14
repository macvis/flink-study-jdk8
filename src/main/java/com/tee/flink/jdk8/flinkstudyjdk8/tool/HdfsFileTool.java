package com.tee.flink.jdk8.flinkstudyjdk8.tool;

import com.tee.flink.jdk8.flinkstudyjdk8.task.HdfsWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.stream.Stream;

/**
 * @author youchao.wen
 * @date 2023/8/12.
 */
@Slf4j
@Component
public class HdfsFileTool {

    public void check(){
        try{
            FileSystem fs = FileSystem.get(new URI("hdfs://47.243.131.115:8020/"));
//            fs.delete(new Path("/user/hive/warehouse/hdfs/demo.csv"), true);
//
            Path dirPath = new Path("/user/hive/warehouse/hdfs/demo.csv");
            FileStatus[] fileStatuses = fs.listStatus(dirPath);
            Stream.of(fileStatuses).forEach(file -> {
                log.info("fileLength = {}", file.getLen());
            });

        }catch(Exception e){
            log.error("", e);
        }
    }

    public void delete(){
        try{
            FileSystem fs = FileSystem.get(new URI("hdfs://47.243.131.115:8020/"));
            fs.delete(new Path("/user/hive/warehouse/hdfs/2023-08-13--21"), true);
        }catch(Exception e){
            log.error("", e);
        }
    }

    public static void main(String[] args) {
        new HdfsFileTool().delete();
    }
}


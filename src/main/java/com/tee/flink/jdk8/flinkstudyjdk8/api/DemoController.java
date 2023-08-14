package com.tee.flink.jdk8.flinkstudyjdk8.api;

import com.tee.flink.jdk8.flinkstudyjdk8.task.FlinkToHdfsTask;
import com.tee.flink.jdk8.flinkstudyjdk8.task.FlinkToHiveSQLTask;
import com.tee.flink.jdk8.flinkstudyjdk8.task.MySqlCdcReadTask;
import com.tee.flink.jdk8.flinkstudyjdk8.task.MySqlJdbcReadTask;
import com.tee.flink.jdk8.flinkstudyjdk8.tool.HdfsFileTool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

/**
 * @author youchao.wen
 * @date 2023/8/7.
 */
@Slf4j
@RestController
@RequestMapping("/flink-demo")
public class DemoController {

    @Autowired
    private MySqlCdcReadTask mySqlCdcReadTask;

    @GetMapping("/mysql-cdc-read")
    public String triggerMySqlCdcRead(){
        CompletableFuture.runAsync(mySqlCdcReadTask :: trigger);
        return "OK";
    }

    @Autowired
    private MySqlJdbcReadTask mySqlJdbcReadTask;
    @GetMapping("/jdbc-read")
    public String triggerMySqlJdbcRead(){
        CompletableFuture.runAsync(mySqlJdbcReadTask :: trigger);
        return "OK";
    }

    @Autowired
    private FlinkToHiveSQLTask flinkToHiveSQLTask;
    @GetMapping("/flink-to-hive")
    public String triggerFlinkToHiveTask(){
        CompletableFuture.runAsync(flinkToHiveSQLTask :: trigger);
        return "OK";
    }

    @Autowired
    private FlinkToHdfsTask flinkToHdfsTask;
    @GetMapping("/flink-to-hdfs")
    public String triggerFlinkToHdfsTask(){
        CompletableFuture.runAsync(flinkToHdfsTask :: trigger);
        return "OK";
    }


    @Autowired
    private HdfsFileTool hdfsFileTool;

    @GetMapping("/file-check")
    public String lengthCheck(){
        CompletableFuture.runAsync(hdfsFileTool:: check);
        return "OK";
    }

    @GetMapping("/file-delete")
    public String fileDelete(){
        hdfsFileTool.delete();

        return "OK";
    }

}

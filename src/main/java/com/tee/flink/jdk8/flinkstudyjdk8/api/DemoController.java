package com.tee.flink.jdk8.flinkstudyjdk8.api;

import com.tee.flink.jdk8.flinkstudyjdk8.task.MySqlCdcReadComponent;
import com.tee.flink.jdk8.flinkstudyjdk8.task.MySqlJdbcReadComponent;
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
    private MySqlCdcReadComponent mySqlCdcReadComponent;

    @GetMapping("/mysql-cdc-read")
    public String triggerMySqlCdcRead(){
        CompletableFuture.runAsync(mySqlCdcReadComponent:: trigger);
        return "OK";
    }

    @Autowired
    private MySqlJdbcReadComponent mySqlJdbcReadComponent;
    @GetMapping("/jdbc-read")
    public String triggerMySqlJdbcRead(){
        CompletableFuture.runAsync(mySqlJdbcReadComponent:: trigger);
        return "OK";
    }
}

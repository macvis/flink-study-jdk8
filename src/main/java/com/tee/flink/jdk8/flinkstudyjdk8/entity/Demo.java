package com.tee.flink.jdk8.flinkstudyjdk8.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author youchao.wen
 * @date 2023/8/9.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Demo {
    private Long id;

    private String actor;

    private String alias;
}

package com.tee.flink.jdk8.flinkstudyjdk8.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author youchao.wen
 * @date 2023/8/9.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Serializable {
    public String name;
    public int id;
}

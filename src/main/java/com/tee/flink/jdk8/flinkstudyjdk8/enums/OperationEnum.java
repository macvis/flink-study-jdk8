package com.tee.flink.jdk8.flinkstudyjdk8.enums;

/**
 * cdc 操作类型枚举
 *
 * @author youchao.wen
 * @date 2023/8/15.
 */
public enum OperationEnum {
    /**
     * 读取
     */
    READ("r"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d"),
    TRUNCATE("t"),
    MESSAGE("m");

    private final String code;

    OperationEnum(String code) {
        this.code = code;
    }
}

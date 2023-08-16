package com.tee.flink.jdk8.flinkstudyjdk8.enums;

import jodd.util.StringUtil;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF;

/**
 * cdc 操作类型枚举
 *
 * @author youchao.wen
 * @date 2023/8/15.
 */
@Getter
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


    public static OperationEnum getByCode(String code){
        for(OperationEnum item : values()){
            if(StringUtils.equals(item.code, code)){
                return item;
            }
        }


        return null;
    }
}

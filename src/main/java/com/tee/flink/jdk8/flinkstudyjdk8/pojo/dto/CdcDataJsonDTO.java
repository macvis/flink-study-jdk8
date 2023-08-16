package com.tee.flink.jdk8.flinkstudyjdk8.pojo.dto;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.io.Serializable;

/**
 * cdc消息的 dto
 *
 * @author youchao.wen
 * @date 2023/8/16.
 */
@Data
public class CdcDataJsonDTO implements Serializable {

    /**
     * 之前的数据，适用于修改、删除等数据已存在的更改
     */
    private JSONObject before;

    /**
     * 之后的消息，适用于新增等数据
     */
    private JSONObject after;

    /**
     * 操作符
     * @see com.tee.flink.jdk8.flinkstudyjdk8.enums.OperationEnum
     */
    private String op;
}

package com.tee.flink.jdk8.flinkstudyjdk8.serde;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tee.flink.jdk8.flinkstudyjdk8.enums.OperationEnum;
import com.tee.flink.jdk8.flinkstudyjdk8.pojo.dto.CdcDataJsonDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
@Slf4j
public class JsonDeserializationSchema implements DeserializationSchema<CdcDataJsonDTO> {
    @Override
    public CdcDataJsonDTO deserialize(byte[] message) throws IOException {
        String str = new String(message);
        System.out.println("接收到的信息 = " + str);
        log.info("cdc消息详情 = {}", str);
        return JSON.parseObject(str, CdcDataJsonDTO.class);
    }

    @Override
    public boolean isEndOfStream(CdcDataJsonDTO nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcDataJsonDTO> getProducedType() {
        return TypeExtractor.getForClass(CdcDataJsonDTO.class);
    }
}

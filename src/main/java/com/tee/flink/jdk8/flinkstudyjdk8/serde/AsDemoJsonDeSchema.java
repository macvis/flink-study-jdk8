package com.tee.flink.jdk8.flinkstudyjdk8.serde;

import com.alibaba.fastjson.JSON;
import com.tee.flink.jdk8.flinkstudyjdk8.entity.Demo;
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
public class AsDemoJsonDeSchema implements DeserializationSchema<Demo> {
    @Override
    public Demo deserialize(byte[] message) throws IOException {
        String str = new String(message);
        log.info("cdc消息详情 = {}", str);
        return JSON.parseObject(str, Demo.class);
    }

    @Override
    public boolean isEndOfStream(Demo nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Demo> getProducedType() {
        return TypeExtractor.getForClass(Demo.class);
    }
}

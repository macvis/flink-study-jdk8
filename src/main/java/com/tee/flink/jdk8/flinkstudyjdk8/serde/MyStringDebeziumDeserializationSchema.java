package com.tee.flink.jdk8.flinkstudyjdk8.serde;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
public class MyStringDebeziumDeserializationSchema implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] bytes) {
        // 将字节数组转换为字符串
        return new String(bytes);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }
}

package com.tee.flink.jdk8.flinkstudyjdk8.serde;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
public class JsonDeserializationSchema implements DeserializationSchema<JSONObject> {
    @Override
    public JSONObject deserialize(byte[] message) throws IOException {
        String str = new String(message);
        System.out.println("接收到的信息 = " + str);
        JSONObject json = JSON.parseObject(str);
        return json.getJSONObject("after");
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeExtractor.getForClass(JSONObject.class);
    }
}

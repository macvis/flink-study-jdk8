package com.tee.flink.jdk8.flinkstudyjdk8.serde;

import com.alibaba.fastjson.JSONObject;
import com.tee.flink.jdk8.flinkstudyjdk8.pojo.dto.CdcDataJsonDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * @author youchao.wen
 * @date 2023/8/16.
 */
@Slf4j
public class KafkaJsonMsgDeserializer implements KafkaRecordDeserializationSchema<CdcDataJsonDTO> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CdcDataJsonDTO> out) throws IOException {


    }

    @Override
    public TypeInformation<CdcDataJsonDTO> getProducedType() {
        return TypeExtractor.getForClass(CdcDataJsonDTO.class);
    }
}

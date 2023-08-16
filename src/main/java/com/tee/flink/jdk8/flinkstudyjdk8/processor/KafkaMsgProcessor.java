package com.tee.flink.jdk8.flinkstudyjdk8.processor;

import com.alibaba.fastjson.JSONObject;
import com.tee.flink.jdk8.flinkstudyjdk8.pojo.dto.CdcDataJsonDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author youchao.wen
 * @date 2023/8/16.
 */
@Slf4j
public class KafkaMsgProcessor extends ProcessFunction<CdcDataJsonDTO, CdcDataJsonDTO> {

    private MapState<Long, List<CdcDataJsonDTO>> bufferState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Long, List<CdcDataJsonDTO>> bufferStateDescriptor = new MapStateDescriptor<>(
                "bufferState",
                TypeInformation.of(new TypeHint<Long>() {}),
                TypeInformation.of(new TypeHint<List<CdcDataJsonDTO>>() {})
        );
        bufferState = getRuntimeContext().getMapState(bufferStateDescriptor);
        log.info("====== KafkaMsgProcessor open ========");
    }


    @Override
    public void processElement(CdcDataJsonDTO value, ProcessFunction<CdcDataJsonDTO, CdcDataJsonDTO>.Context ctx, Collector<CdcDataJsonDTO> out) throws Exception {
        long offset = ctx.timestamp();
        log.info("====== KafkaMsgProcessor processElement offset={}", offset);

        // 检查是否有待处理的消息，如果有，则将当前消息添加到缓冲区中
        if (bufferState.contains(offset)) {
            List<CdcDataJsonDTO> buffer = bufferState.get(offset);
            buffer.add(value);
            bufferState.put(offset, buffer);
        } else {
            // 当前消息是第一个消息，可以直接输出
            out.collect(value);

            // 创建新的缓冲区并将当前消息添加到缓冲区中
            List<CdcDataJsonDTO> buffer = new ArrayList<>();
            buffer.add(value);
            bufferState.put(offset, buffer);
        }

        // 检查是否有已排序的连续消息可以输出
        checkAndOutputSortedRecords(out);
    }

    private void checkAndOutputSortedRecords(Collector<CdcDataJsonDTO> out) throws Exception {
        // 获取所有缓冲区中最小的 offset
        Iterator<Map.Entry<Long, List<CdcDataJsonDTO>>> iterator = bufferState.iterator();
        long minOffset = iterator.next().getKey();
        while (iterator.hasNext()) {
            long currentOffset = iterator.next().getKey();
            if (currentOffset < minOffset) {
                minOffset = currentOffset;
            }
        }

        // 输出所有连续的消息，并从缓冲区中移除
        List<CdcDataJsonDTO> sortedRecords = bufferState.get(minOffset);
        while (sortedRecords != null) {
            CdcDataJsonDTO dto = sortedRecords.get(0);
            out.collect(dto);

            bufferState.remove(minOffset);

            minOffset++;
            sortedRecords = bufferState.get(minOffset);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        bufferState.clear();
    }
}

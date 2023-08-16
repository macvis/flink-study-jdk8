package com.tee.flink.jdk8.flinkstudyjdk8.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
@Deprecated
public class HiveJdbcCheckpointFunction implements MapFunction<String, Integer>, CheckpointedFunction {

    private transient ListState<Integer> state;
    private boolean isRestored = false;

    @Override
    public Integer map(String value) throws Exception {
        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}

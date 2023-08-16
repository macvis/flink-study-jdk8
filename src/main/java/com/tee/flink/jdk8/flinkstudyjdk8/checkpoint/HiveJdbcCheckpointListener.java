package com.tee.flink.jdk8.flinkstudyjdk8.checkpoint;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;


/**
 * @author youchao.wen
 * @date 2023/8/15.
 */
public class HiveJdbcCheckpointListener implements CheckpointListener {

    private final CheckpointFailureManager failureManager;

    public HiveJdbcCheckpointListener(CheckpointFailureManager failureManager) {
        this.failureManager = failureManager;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }
}

package com.opensource.flinkcdcservice.common.env;

import com.opensource.flinkcdcservice.common.commons.ParamConstants;
import com.opensource.flinkcdcservice.common.utils.ParameterWrapper;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.management.StandardEmitterMBean;

public class EnvWrapper {

    @Getter
    private final StreamExecutionEnvironment environment;

    public EnvWrapper() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        this.environment = new StreamExecutionEnvironment(config);
    }

    public void setGlobalConfig(ParameterWrapper parameter) {
        // 设置任务参数到全局变量，展示在jobs/{jobId}/config中
        environment.getConfig().setGlobalJobParameters(parameter.getParameterTool());
        environment.setParallelism(parameter.getInteger(ParamConstants.PARALLELISM));
        // 配置位点信息
        this.setCheckpoint(parameter);
        this.setRetryPolicy();
    }

    private void setCheckpoint(ParameterWrapper parameter) {
        this.environment.enableCheckpointing(60000);
        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if ("AT_LEAST_ONCE".toLowerCase().equals(parameter.getString(ParamConstants.JOB_CHECKPOINT_MODE))) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        this.environment.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        this.environment.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        this.environment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
    }

    private void setRetryPolicy() {
        this.environment.setRestartStrategy(RestartStrategies.exponentialDelayRestart(Time.seconds(5),
                Time.minutes(1), 2.0, Time.hours(1), 0.1));
    }

    public  <T> DataStreamSource<T> buildSource(Source<T, ?, ?> source, String sourceName) {
        return this.environment.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName);
    }

    public <T> void buildSink(DataStreamSource<T> dataStream, RichSinkFunction<T> sinkFunction,  String sinkName) {
        dataStream.addSink(sinkFunction).name(sinkName);
    }

    public void executeJob(String jobName) throws Exception {
        environment.execute(jobName);
    }
}

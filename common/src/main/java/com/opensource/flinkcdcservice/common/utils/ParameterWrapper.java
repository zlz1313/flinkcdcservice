package com.opensource.flinkcdcservice.common.utils;

import lombok.Getter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;

public final class ParameterWrapper {

    @Getter
    private final ParameterTool parameterTool;

    public ParameterWrapper(String[] args) {
        this.parameterTool = ParameterTool.fromArgs(args);
    }

    public String getString(ConfigOption<String> configOption) {
        return parameterTool.get(configOption.key(), configOption.defaultValue());
    }

    public boolean getBool(ConfigOption<Boolean> configOption) {
        return parameterTool.getBoolean(configOption.key(), configOption.defaultValue());
    }

    public int getInteger(ConfigOption<Integer> configOption) {
        return parameterTool.getInt(configOption.key(), configOption.defaultValue());
    }

    public double getDouble(ConfigOption<Double> configOption) {
        return parameterTool.getDouble(configOption.key(), configOption.defaultValue());
    }

    public String get(String key, String defaultValue) {
        return this.parameterTool.get(key, defaultValue);
    }

}

package com.delta.deltalake.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.LinkedHashMap;
import java.util.Map;

public final class BenchmarkResult {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Map<String, Object> values = new LinkedHashMap<>();

    public BenchmarkResult(String experiment, String backend) {
        values.put("experiment", experiment);
        values.put("backend", backend);
    }

    public BenchmarkResult put(String key, Object value) {
        values.put(key, value);
        return this;
    }

    public Map<String, Object> values() {
        return Map.copyOf(values);
    }

    public String toJson() throws Exception {
        return MAPPER.writeValueAsString(values);
    }
}

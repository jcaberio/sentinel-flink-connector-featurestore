package com.maya.risktech;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

public class FeatureStoreDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema resolvedSchema;
    private final ReadableConfig config;

    public FeatureStoreDynamicTableSink(ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.resolvedSchema = resolvedSchema;
        this.config = config;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new FeatureStoreSink<>(resolvedSchema, config));
    }

    @Override
    public DynamicTableSink copy() {
        return new FeatureStoreDynamicTableSink(resolvedSchema, config);
    }

    @Override
    public String asSummaryString() {
        return "SagemakerFeatureStore";
    }
}

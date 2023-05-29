package com.maya.risktech;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

public class FeatureStoreDynamicSinkFactory implements DynamicTableSinkFactory {

    public static final String FACTORY_IDENTIFIER = "featurestore";

    public static final ConfigOption<String> FEATURE_GROUP_NAME = ConfigOptions.key("feature-group-name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the feature group that you want to insert the record into.");

    public static final ConfigOption<String> AWS_REGION = ConfigOptions.key("aws.region")
            .stringType()
            .noDefaultValue()
            .withDescription("AWS region for the destination FeatureStore.");
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new FeatureStoreDynamicTableSink(resolvedSchema, config);
    }

    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(FEATURE_GROUP_NAME);
    }

    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(AWS_REGION);
    }
}

package com.maya.risktech;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.SageMakerFeatureStoreRuntimeClient;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.FeatureValue;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.PutRecordRequest;
import software.amazon.awssdk.services.sagemakerfeaturestoreruntime.model.PutRecordResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.RowData.createFieldGetter;

@Slf4j
public class FeatureStoreSink<IN> extends RichSinkFunction<IN> {

    private final ReadableConfig config;
    private final DataType physicalRowDataType;
    private transient SageMakerFeatureStoreRuntimeClient client;

    public FeatureStoreSink(ResolvedSchema resolvedSchema, ReadableConfig config) {
        this.config = config;
        this.physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
    }

    @Override
    public void open(Configuration parameters) {
        log.info("Initializing SageMakerFeatureStoreRuntimeClient");
        this.client = SageMakerFeatureStoreRuntimeClient.create();
    }

    @Override
    public void close() {
        log.info("Closing SageMakerFeatureStoreRuntimeClient");
        this.client.close();
    }

    @Override
    public void invoke(IN input, Context context) {
        RowData rowData = (RowData) input;
        String featureGroupName = this.config.get(FeatureStoreDynamicSinkFactory.FEATURE_GROUP_NAME);
        Map<String, Object> map = new HashMap<>();

        List<DataTypes.Field> fields = DataType.getFields(physicalRowDataType);
        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter = createFieldGetter(field.getDataType().getLogicalType(), i);
            Object data = fieldGetter.getFieldOrNull(rowData);
            map.put(field.getName(), data);
        }

        List<FeatureValue> record = new ArrayList<>();
        map.forEach((k,v) -> record.add(FeatureValue.builder().featureName(k).valueAsString(String.valueOf(v)).build()));
        PutRecordRequest recordRequest = PutRecordRequest.builder().featureGroupName(featureGroupName).record(record).build();
        log.info("PutRecordRequest: {}", recordRequest);
        log.info("Calling SageMakerFeatureStoreRuntimeClient.putRecord");
        PutRecordResponse response = this.client.putRecord(recordRequest);
        log.info("PutRecordResponse: {}", response.sdkHttpResponse().statusText().orElse("FAIL"));
    }
}

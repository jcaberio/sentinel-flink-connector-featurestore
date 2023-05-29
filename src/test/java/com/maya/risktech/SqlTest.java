package com.maya.risktech;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

@Slf4j
public class SqlTest {

    @Test
    public void testSql() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "CREATE TABLE feature_group (\n" +
                "\tmsisdn STRING,\n" +
                "\ttransmission_date_and_time STRING,\n" +
                "\ttotal_amount_1h DOUBLE,\n" +
                "\ttotal_amount_1d DOUBLE,\n" +
                "\ttotal_amount_30d DOUBLE,\n" +
                "\tnum_send_all_count_1d INT,\n" +
                "\tnum_send_all_count_30d INT\n" +
                ") WITH (\n" +
                "\t'connector' = 'featurestore',\n" +
                "\t'aws.region' = 'ap-southeast-1',\n" +
                "\t'feature-group-name' = 'dev-risktech-realtime-instapay-send-features'\n" +
                ")";
        log.info(ddl);
        tEnv.executeSql(ddl);
        String sql = "INSERT INTO feature_group values ('+639191234570', '2023-05-28T14:45:57Z',100.0, 200.0, 300.0, 400, 500)";
        log.info(sql);
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().map(JobClient::getJobExecutionResult).orElseThrow().get();
    }
}

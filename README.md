## sentinel-flink-connector-featurestore

[Flink](https://nightlies.apache.org/flink/flink-docs-release-1.17) [connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/overview/) for [writing data](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sourcessinks/) into [Amazon SageMaker Feature Store](https://docs.aws.amazon.com/sagemaker/latest/dg/feature-store.html)

```sql
CREATE TABLE feature_group_table (
	msisdn STRING,
	transmission_date_and_time STRING,
	total_amount_1h DOUBLE,
	total_amount_1d DOUBLE,
	total_amount_30d DOUBLE,
	num_send_all_count_1d INT,
	num_send_all_count_30d INT
) WITH (
	'connector' = 'featurestore',
	'aws.region' = 'ap-southeast-1',
	'feature-group-name' = 'my_feature_group'
)
```
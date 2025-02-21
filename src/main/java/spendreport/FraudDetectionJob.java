package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Get Kafka configuration from environment
		String brokers = System.getenv("KAFKA_ENDPOINT") != null ?
				System.getenv("KAFKA_ENDPOINT") : "localhost:29092";

		String topic = "events";

		Properties props = new Properties();
		String username = System.getenv("KAFKA_USERNAME");
		String password = System.getenv("KAFKA_PASSWORD");

		if (username != null && password != null) {
			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
			props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
			props.put(SaslConfigs.SASL_JAAS_CONFIG,String.format("""
                org.apache.kafka.common.security.scram.ScramLoginModule required \
                username='%s' \
                password='%s';
                """, username, password));
		}

		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		DataStream<Alert> alerts = transactions
				.keyBy(Transaction::getAccountId)
				.process(new FraudDetector())
				.name("fraud-detector");

		KafkaSink<Alert> kafkaSink = KafkaSink.<Alert>builder()
				.setBootstrapServers(brokers)
				.setKafkaProducerConfig(props)
				.setRecordSerializer(KafkaRecordSerializationSchema.<Alert>builder()
						.setTopic(topic)
						.setValueSerializationSchema((Alert alert) ->
								alert.toString().getBytes())
						.build())
				.build();

		alerts.sinkTo(kafkaSink).name("kafka-alerts-sink");

		env.execute("Fraud Detection");
	}
}
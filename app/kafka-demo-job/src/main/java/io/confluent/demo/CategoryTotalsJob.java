package io.confluent.demo;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Reads the Avro/Schema-Registry topic `demo_events` (written by the SQL demo pipeline),
 * keeps running per-category totals, and writes them as JSON to the topic
 * `java_category_totals` (an upsert-kafka topic keyed by category).
 *
 * Args (all optional):
 *   --bootstrap       Kafka bootstrap servers   (default: in-cluster plaintext listener)
 *   --schema-registry Schema Registry URL       (default: in-cluster service)
 *   --source-topic    input topic               (default: demo_events)
 *   --sink-topic      output topic              (default: java_category_totals)
 * Env KAFKA_USERNAME / KAFKA_PASSWORD (optional): enable SASL/PLAIN for source and sink.
 */
public class CategoryTotalsJob {

  public static void main(String[] args) throws Exception {
    String bootstrap = arg(args, "--bootstrap", "kafka.confluent.svc.cluster.local:9071");
    String registry = arg(args, "--schema-registry", "http://schemaregistry.confluent.svc.cluster.local:8081");
    String source = arg(args, "--source-topic", "demo_events");
    String sink = arg(args, "--sink-topic", "java_category_totals");

    // Kafka credentials arrive as env vars (from a k8s secret) when the cluster requires SASL/PLAIN.
    String user = System.getenv("KAFKA_USERNAME");
    String password = System.getenv("KAFKA_PASSWORD");
    String sasl = "";
    if (user != null && !user.isEmpty()) {
      sasl =
          ",\n  'properties.security.protocol' = 'SASL_PLAINTEXT',\n"
              + "  'properties.sasl.mechanism' = 'PLAIN',\n"
              + "  'properties.sasl.jaas.config' = "
              + "'org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
              + user + "\" password=\"" + password + "\";'";
    }

    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    tEnv.executeSql(
        "CREATE TABLE events (\n"
            + "  `id` STRING,\n"
            + "  `value` INT,\n"
            + "  `event_time` TIMESTAMP(3),\n"
            + "  `category` STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = '" + source + "',\n"
            + "  'properties.bootstrap.servers' = '" + bootstrap + "',\n"
            + "  'properties.group.id' = 'java-category-totals',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'value.format' = 'avro-confluent',\n"
            + "  'value.avro-confluent.url' = '" + registry + "'" + sasl + "\n"
            + ")");

    tEnv.executeSql(
        "CREATE TABLE category_totals (\n"
            + "  `category` STRING,\n"
            + "  `total_value` BIGINT,\n"
            + "  `event_count` BIGINT,\n"
            + "  PRIMARY KEY (`category`) NOT ENFORCED\n"
            + ") WITH (\n"
            + "  'connector' = 'upsert-kafka',\n"
            + "  'topic' = '" + sink + "',\n"
            + "  'properties.bootstrap.servers' = '" + bootstrap + "',\n"
            + "  'key.format' = 'json',\n"
            + "  'value.format' = 'json'" + sasl + "\n"
            + ")");

    // Application mode: submitting the INSERT starts the (unbounded) job, which then runs
    // until the FlinkApplication is stopped.
    tEnv.executeSql(
        "INSERT INTO category_totals\n"
            + "SELECT `category`, CAST(SUM(`value`) AS BIGINT), COUNT(*) FROM events GROUP BY `category`");
  }

  private static String arg(String[] args, String name, String def) {
    for (int i = 0; i < args.length - 1; i++) {
      if (name.equals(args[i])) return args[i + 1];
    }
    return def;
  }
}

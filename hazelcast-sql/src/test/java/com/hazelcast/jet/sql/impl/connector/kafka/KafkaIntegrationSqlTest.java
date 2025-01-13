/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.core.HazelcastJsonValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaIntegrationSqlTest extends KafkaSqlTestSupport {

    @Test
    public void should_read_from_imap_to_kafka() {
        String topicName = "testTopic";
        kafkaTestSupport.createTopic(topicName, 1);
        createConfluentKafkaMapping(topicName);
        String mapName = "testMap";
        createMap(mapName);

        executeSql("INSERT INTO " + mapName + " VALUES\n" +
                "  (1, 'ABCD', 5.5, 10),\n" +
                "  (2, 'EFGH', 14, 20);");

        Map<Integer, HazelcastJsonValue> map1 = new HashMap<>();
        map1.put(1, new HazelcastJsonValue("{\"ticker\":\"ABCD\",\"price\":\"5.5\",\"amount\":10}"));
        map1.put(2, new HazelcastJsonValue("{\"ticker\":\"EFGH\",\"price\":\"14\",\"amount\":20}"));
        assertMapContents(mapName, map1);

        executeSql("CREATE JOB testJob\n" +
                "AS\n" +
                "SINK INTO " + topicName + "\n" +
                "SELECT __key, ticker, price, amount FROM " + mapName);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("ticker", "ABCD");
        map2.put("price", "5.5");
        map2.put("amount", 10);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("ticker", "EFGH");
        map3.put("price", "14");
        map3.put("amount", 20);
        assertRowsEventuallyInAnyOrder(
                "SELECT __key,this FROM " + topicName,
                Arrays.asList(new Row(1, map2), new Row(2, map3)));

        try (KafkaConsumer<Integer, String> consumer = kafkaTestSupport.createConsumer(topicName)) {
            Map<Integer, String> map4 = new HashMap<>();
            map4.put(1, "{\"ticker\":\"ABCD\",\"price\":\"5.5\",\"amount\":10}");
            map4.put(2, "{\"ticker\":\"EFGH\",\"price\":\"14\",\"amount\":20}");
            assertTopicContentsEventually(consumer, map4);
        }
    }

    private static void assertMapContents(String mapName, Map<Integer, HazelcastJsonValue> expected) {
        var mapContents = new HashMap<>(instance().getMap(mapName));
        assertTrueEventually(() -> assertThat(mapContents).containsAllEntriesOf(expected));
    }

    private void createMap(String mapName) {
        executeSql("CREATE OR REPLACE MAPPING " + mapName + " (\n" +
                "            __key INT,\n" +
                "            ticker VARCHAR,\n" +
                "            price DECIMAL,\n" +
                "            amount BIGINT)\n" +
                "        TYPE IMap\n" +
                "        OPTIONS (\n" +
                "            'keyFormat'='int',\n" +
                "    'valueFormat'='json-flat'\n" +
                ");");
    }

    private void createConfluentKafkaMapping(String topicName) {
        String createMappingQuery =
                format("CREATE OR REPLACE MAPPING %s (\n" +
                        "                    __key INT,\n" +
                        "                    ticker VARCHAR,\n" +
                        "                    price DECIMAL,\n" +
                        "                    amount BIGINT)\n" +
                        "        TYPE Kafka\n" +
                        "        OPTIONS (\n" +
                        "            'keyFormat'='int',\n" +
                        "            'valueFormat' = 'json-flat',\n" +
                        "            'bootstrap.servers' = '%s',\n" +
                        "            'auto.offset.reset' = 'earliest',\n" +
                        "            'session.timeout.ms' = '45000',\n" +
                        "            'acks' = 'all'\n" +
                        ");", topicName, kafkaTestSupport.getBrokerConnectionString());
        executeSql(createMappingQuery);
    }

    private void executeSql(String query) {
        logger.info("Execute sql: " + query);
        try {
            try (var ignored = sqlService.execute(query)) {
                //nop
            }
        } catch (Exception ex) {
            logger.warning("Error while executing SQL: " + query, ex);
            throw ex;
        }
    }

    public void assertTopicContentsEventually(KafkaConsumer<Integer, String> consumer, Map<Integer, String> expected) {
        HashMap<Integer, String> collected = new HashMap<Integer, String>();
        assertTrueEventually(() -> {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled records: " + records.count());
            for (ConsumerRecord<Integer, String> record : records) {
                collected.put(record.key(), record.value());
            }
            assertThat(collected).containsAllEntriesOf(expected);
        });
    }
}

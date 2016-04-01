/**
 * * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.File;
import java.util.Properties;


public class PageViewsRegionBranchExample {
    static final String SOME_REGION = "Europe";

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "pageview-region-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);

        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStreamBuilder builder = new KStreamBuilder();

        // See `pageview.avsc` under `src/main/avro/`.
        KStream<String, GenericRecord> views = builder.stream("PageViews");

        KStream<String, GenericRecord> viewsByUser = views.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
            @Override
            public KeyValue<String, GenericRecord> apply(String dummy, GenericRecord record) {
                return new KeyValue<>((String) record.get("user"), record);
            }
        });

        // See `userprofile.avsc` under `src/main/avro/`.
        KTable<String, GenericRecord> users = builder.table("UserProfiles");

        KTable<String, String> userRegions = users.mapValues(new ValueMapper<GenericRecord, String>() {
            @Override
            public String apply(GenericRecord record) {
                return (String) record.get("region");
            }
        });

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region
        // (see below).
        Schema schema = new Schema.Parser().parse(new File("pageviewregion.avsc"));

        KStream<String, GenericRecord>[] regionStream = viewsByUser
                .leftJoin(userRegions, new ValueJoiner<GenericRecord, String, GenericRecord>() {
                    @Override
                    public GenericRecord apply(GenericRecord view, String region) {
                        GenericRecord viewRegion = new GenericData.Record(schema);
                        viewRegion.put("user", view.get("user"));
                        viewRegion.put("page", view.get("page"));
                        viewRegion.put("region", region);
                        return viewRegion;
                    }
                })
                .branch(new Predicate<String, GenericRecord>() {
                    @Override
                    public boolean test(String key, GenericRecord value) {
                        // no record is discarded
                        return true;
                    }
                });

        // write to result topic
        regionStream[0].to("pageview-region-stream");

        // filter and write to result topic
        regionStream[1].filter(new Predicate<String, GenericRecord>() {
            @Override
            public boolean test(String key, GenericRecord value) {
                return value.get("region").equals(SOME_REGION);
            }
        }).to("filtered-stream");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}
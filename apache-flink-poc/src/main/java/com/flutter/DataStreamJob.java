/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flutter;


import com.flutter.serializers.BetTupleSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	private static final String SOURCE_TOPIC = "poc-source";
	private static final String SINK_TOPIC = "poc-sink";
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String CONSUMER_GROUP = "kafkaFlinkPOC";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> stringInputSteam = env.fromSource(createKafkaSource(), WatermarkStrategy.noWatermarks(), "Kafka Source");
		KeyedStream<Tuple2<String, Integer>, String> keyedStream =
				stringInputSteam.map(DataStreamJob::mapToTuple)
						.keyBy(betTuple -> betTuple.f0);

		SingleOutputStreamOperator<Tuple2<String, Integer>> operator = keyedStream.sum(1);

		operator.sinkTo(createKafkaSink());
		// Execute program, beginning computation.
		env.execute("Flink Kafka POC");
	}

	private static Tuple2<String, Integer> mapToTuple(String betString) throws JsonProcessingException {
		JsonNode jsonNode = OBJECT_MAPPER.readTree(betString);

		return new Tuple2Builder<String, Integer>()
				.add(jsonNode.get("runnerName").asText(), jsonNode.get("liability").asInt())
				.build()[0];
	}

	private static KafkaSource<String> createKafkaSource(){
		return KafkaSource.<String>builder()
				.setBootstrapServers(BOOTSTRAP_SERVERS)
				.setTopics(SOURCE_TOPIC)
				.setGroupId(CONSUMER_GROUP)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}
	private static KafkaSink<Tuple2<String, Integer>> createKafkaSink(){
		return KafkaSink.<Tuple2<String, Integer>>builder()
				.setBootstrapServers(BOOTSTRAP_SERVERS)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(SINK_TOPIC)
						.setValueSerializationSchema(new BetTupleSerializer())
						.build()
				)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
	}
}

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


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

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
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForSourceTopic();

		DataStream<String> stringInputSteam = env.addSource(flinkKafkaConsumer);

		FlinkKafkaProducer<String> flinkKafkaProducer = createStringProducerForSinkTopic();

		stringInputSteam.addSink(flinkKafkaProducer);
		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	private static FlinkKafkaConsumer<String> createStringConsumerForSourceTopic(){
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DataStreamJob.BOOTSTRAP_SERVERS);
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DataStreamJob.CONSUMER_GROUP);

		return new FlinkKafkaConsumer<>(DataStreamJob.SOURCE_TOPIC, new SimpleStringSchema(), props);
	}

	private static FlinkKafkaProducer<String> createStringProducerForSinkTopic(){
		return new FlinkKafkaProducer<>(DataStreamJob.BOOTSTRAP_SERVERS, DataStreamJob.SINK_TOPIC, new SimpleStringSchema());
	}
}

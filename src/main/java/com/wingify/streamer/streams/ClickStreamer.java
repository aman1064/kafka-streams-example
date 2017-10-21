package com.wingify.streamer.streams;

import java.util.Properties;

import com.wingify.streamer.models.ClickStream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.wingify.streamer.Main;
import com.wingify.streamer.collectors.ClickStreamCollector;
import com.wingify.streamer.serializer.JsonDeserializer;
import com.wingify.streamer.serializer.JsonSerializer;
import com.wingify.streamer.utils.ClickStreamPartitioner;
/*
This class implements Runnable and all business logic of Kafka Topology is defined here.
This uses a high level Streams DSL which is very similar to JAVA 8 lambda exppressions.
It defines the source , sink, filtering, aggregation logic etc.
 */

public class ClickStreamer implements Runnable {

	private static Properties getProperties() {
		Properties props = Main.STREAMING_PROPERTIES;
		//By default we are using processig time windows. This can be manually defined by implementing own class and can be extracted from consumer record
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		return props;
	}

	@Override
	public void run() {
		StreamsConfig streamingConfig = new StreamsConfig(getProperties());
		JsonSerializer<ClickStreamCollector> clickAggregationSerializer = new JsonSerializer<>();
		JsonDeserializer<ClickStreamCollector> clickAggregationDeSerializer = new JsonDeserializer<>(
				ClickStreamCollector.class);

		JsonDeserializer<ClickStream> clickStreamDeserializer = new JsonDeserializer<>(ClickStream.class);
		JsonSerializer<ClickStream> clickStreamSerializer = new JsonSerializer<>();

		Serde<ClickStream> clickStreamSerde = Serdes.serdeFrom(clickStreamSerializer, clickStreamDeserializer);

		StringSerializer stringSerializer = new StringSerializer();
		StringDeserializer stringDeserializer = new StringDeserializer();

		Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);
		Serde<ClickStreamCollector> collectorSerde = Serdes.serdeFrom(clickAggregationSerializer,
				clickAggregationDeSerializer);

		WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
		WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);

		Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

		KStreamBuilder kStreamBuilder = new KStreamBuilder();

		//Defining Source Streams from multiple topics.
		KStream<String, ClickStream> clickStream = kStreamBuilder.stream(stringSerde, clickStreamSerde,
				Main.TOPIC_PROPERTIES.getProperty("topic.click.input").split(","));

		//Kafka Streams DSL in action with filtering and cleaning logic and passing it through aggregation collector
		clickStream
				.filter((k,v) -> (v!=null))
				.map((k, v) -> 
						new KeyValue<>(v.getSessionId()+"_"+v.getUuid(),v))
				.through(stringSerde, clickStreamSerde, Main.TOPIC_PROPERTIES.getProperty("topic.click.output"))
				.groupBy((k, v) -> k, stringSerde, clickStreamSerde)
				.aggregate(ClickStreamCollector::new, (k, v, clickStreamCollector) -> clickStreamCollector.add(v),
						TimeWindows.of(1 * 60 * 1000), collectorSerde,
						Main.TOPIC_PROPERTIES.getProperty("topic.click.aggregation"))
				.to(windowedSerde, collectorSerde, new ClickStreamPartitioner(), Main.TOPIC_PROPERTIES.getProperty("topic.click.summary"));

		System.out.println("Starting Click streamer");
		KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder, streamingConfig);
		kafkaStreams.start();
		System.out.println("Started Click streamer");
	}
	

}

package com.wingify.streamer.utils;

import java.util.Random;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StreamPartitioner;

import com.wingify.streamer.collectors.ClickStreamCollector;

//Custom partitioning logic. Just an example that will return value between 0-7 hence applies for 8 paritions
public class ClickStreamPartitioner implements StreamPartitioner<Windowed<String>, ClickStreamCollector> {

	@Override
	public Integer partition(Windowed<String> key, ClickStreamCollector value, int numPartitions) {
		try {
			String uuid = value.getUuid();
			Integer number = Integer.valueOf(uuid.substring(uuid.length() - 2, uuid.length()), 16);
			return (number % 32) / (32/numPartitions);
		} catch (Exception ex) {
			ex.printStackTrace();
			return new Random().nextInt(numPartitions);
		}

	}

}


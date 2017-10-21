package org.streams.demo.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {

	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public byte[] serialize(String topic, T t) {
		try {
			return mapper.writeValueAsBytes(t);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() {

	}
}

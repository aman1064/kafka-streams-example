package org.streams.demo.serializer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {

	private ObjectMapper mapper = new ObjectMapper();
	private Class<T> deserializedClass;

	public JsonDeserializer(Class<T> deserializedClass) {
		this.deserializedClass = deserializedClass;
	}

	public JsonDeserializer() {
	}

	@Override
	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> map, boolean b) {
		if (deserializedClass == null) {
			deserializedClass = (Class<T>) map.get("serializedClass");
		}
	}

	@Override
	public T deserialize(String s, byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		try {
			return mapper.readValue(bytes, deserializedClass);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("CORRUPT MESSAGE :: "+ new String(bytes));
		}
		return null;
	}

	@Override
	public void close() {

	}

}

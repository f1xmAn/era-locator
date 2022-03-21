package com.github.f1xman.era;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.SneakyThrows;

public class SerDe {

    private static final JsonMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();

    @SneakyThrows
    public static <T> T deserialize(byte[] raw, Class<T> clazz) {
        return mapper.readValue(raw, clazz);
    }

    @SneakyThrows
    public static <T> byte[] serialize(T value) {
        return mapper.writeValueAsBytes(value);
    }

}

package com.github.f1xman.era;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

class DataAccessEventDeserializationSchema extends AbstractDeserializationSchema<DataAccessEvent> {
    @Override
    public DataAccessEvent deserialize(byte[] bytes) {
        return DataAccessEvent.from(bytes);
    }
}

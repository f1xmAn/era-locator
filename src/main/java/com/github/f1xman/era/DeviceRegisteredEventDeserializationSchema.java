package com.github.f1xman.era;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

class DeviceRegisteredEventDeserializationSchema extends AbstractDeserializationSchema<DeviceRegisteredEvent> {
    @Override
    public DeviceRegisteredEvent deserialize(byte[] bytes) {
        return DeviceRegisteredEvent.from(bytes);
    }
}

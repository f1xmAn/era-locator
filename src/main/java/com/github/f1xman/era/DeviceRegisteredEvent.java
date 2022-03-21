package com.github.f1xman.era;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.time.OffsetDateTime;

import static lombok.AccessLevel.PRIVATE;

@FieldDefaults(level = PRIVATE, makeFinal = true)
@Getter
@ToString
@RequiredArgsConstructor(onConstructor_ = {@JsonCreator})
public class DeviceRegisteredEvent {

    @JsonProperty("imei")
    String imei;
    @JsonProperty("phoneNumber")
    String phoneNumber;
    @JsonProperty("station")
    Station station;
    @JsonProperty("registeredAt")
    OffsetDateTime registeredAt;

    public static DeviceRegisteredEvent from(byte[] raw) {
        return SerDe.deserialize(raw, DeviceRegisteredEvent.class);
    }

    public static DeviceRegisteredEvent from(TypedValue value) {
        return SerDe.deserialize(value.getValue().toByteArray(), DeviceRegisteredEvent.class);
    }

    public TypedValue toTypedValue() {
        return TypedValue.newBuilder()
                .setHasValue(true)
                .setValue(ByteString.copyFrom(SerDe.serialize(this)))
                .setTypename("com.github.f1xman.era.anomalydetection.device/DeviceRegisteredEvent")
                .build();
    }

    @Getter
    @ToString
    @FieldDefaults(level = PRIVATE, makeFinal = true)
    @RequiredArgsConstructor(onConstructor_ = {@JsonCreator})
    public static class Station {

        @JsonProperty("latitude")
        double latitude;
        @JsonProperty("longitude")
        double longitude;

    }
}

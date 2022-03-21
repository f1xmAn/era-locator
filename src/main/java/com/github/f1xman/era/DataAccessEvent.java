package com.github.f1xman.era;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.net.URL;

import static lombok.AccessLevel.PRIVATE;

@ToString
@Getter
@FieldDefaults(level = PRIVATE, makeFinal = true)
@RequiredArgsConstructor(onConstructor_ = {@JsonCreator})
public class DataAccessEvent {

    @JsonProperty("imei")
    String imei;
    @JsonProperty("dataUrl")
    URL dataUrl;

    public static DataAccessEvent from(byte[] bytes) {
        return SerDe.deserialize(bytes, DataAccessEvent.class);
    }

    public TypedValue toTypedValue() {
        return TypedValue.newBuilder()
                .setTypename("com.github.f1xman.era.anomalydetection.device/DataAccessEvent")
                .setHasValue(true)
                .setValue(ByteString.copyFrom(SerDe.serialize(this)))
                .build();
    }

}

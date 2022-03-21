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

package com.github.f1xman.era;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

import static org.apache.flink.statefun.flink.datastream.RequestReplyFunctionBuilder.requestReplyFunctionBuilder;

public class StreamingJob {

    public static final FunctionType DEVICE_FUNCTION = new FunctionType("com.github.f1xman.era.anomalydetection.device", "Device");
    public static final FunctionType PHONE_FUNCTION = new FunctionType("com.github.f1xman.era.anomalydetection.phone", "Phone");
    public static final URI ENDPOINT = URI.create("http://localhost:8080/statefun");
    public static final EgressIdentifier<TypedValue> SUSPICIOUS_DEVICE_REGISTERED_EGRESS = new EgressIdentifier<>(
            "com.github.f1xman.era.anomalydetection.device",
            "SuspiciousDeviceRegisteredEgress",
            TypedValue.class
    );

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RoutableMessage> deviceRegisteredEventsIngress = env.fromSource(
                        deviceRegisteredEventsSource(),
                        WatermarkStrategy.noWatermarks(),
                        "deviceRegisteredEventsSource")
                .map(e -> RoutableMessageBuilder.builder()
                        .withTargetAddress(DEVICE_FUNCTION, e.getImei())
                        .withMessageBody(e.toTypedValue())
                        .build()
                );
        DataStream<RoutableMessage> dataAccessEventsIngress = env.fromSource(
                        dataAccessEventsSource(),
                        WatermarkStrategy.noWatermarks(),
                        "dataAccessEventsSource")
                .map(e -> RoutableMessageBuilder.builder()
                        .withTargetAddress(DEVICE_FUNCTION, e.getImei())
                        .withMessageBody(e.toTypedValue())
                        .build()
                );
        StatefulFunctionEgressStreams statefunStreams = StatefulFunctionDataStreamBuilder.builder("find-location")
                .withDataStreamAsIngress(deviceRegisteredEventsIngress)
                .withDataStreamAsIngress(dataAccessEventsIngress)
                .withRequestReplyRemoteFunction(requestReplyFunctionBuilder(DEVICE_FUNCTION, ENDPOINT))
                .withRequestReplyRemoteFunction(requestReplyFunctionBuilder(PHONE_FUNCTION, ENDPOINT))
                .withEgressId(SUSPICIOUS_DEVICE_REGISTERED_EGRESS)
                .build(env);
        DataStream<TypedValue> suspiciousEvents = statefunStreams.getDataStreamForEgressId(SUSPICIOUS_DEVICE_REGISTERED_EGRESS);
        suspiciousEvents
                .map(DeviceRegisteredEvent::from)
                .keyBy(DeviceRegisteredEvent::getImei)
                .countWindow(3, 1)
                .process(new FindLocationFunction())
                .print();

        env.execute("Era Locator");
    }

    private static KafkaSource<DataAccessEvent> dataAccessEventsSource() {
        return KafkaSource.<DataAccessEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("data-access-events")
                .setGroupId("era-stream-locator")
                .setValueOnlyDeserializer(new DataAccessEventDeserializationSchema())
                .build();
    }

    private static KafkaSource<DeviceRegisteredEvent> deviceRegisteredEventsSource() {
        return KafkaSource.<DeviceRegisteredEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("device-registered-events")
                .setGroupId("era-stream-locator")
                .setValueOnlyDeserializer(new DeviceRegisteredEventDeserializationSchema())
                .build();
    }

}

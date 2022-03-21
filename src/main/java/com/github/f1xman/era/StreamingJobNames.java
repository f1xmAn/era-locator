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

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.statefun.flink.datastream.RequestReplyFunctionBuilder.requestReplyFunctionBuilder;

public class StreamingJobNames {

    public static final FunctionType DEVICE = new FunctionType("com.github.f1xman.era.anomalydetection.device", "DeviceFunction");
    public static final FunctionType LOCAL_GREET = new FunctionType("example", "local-greet");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        DataStreamSource<String> names = env.addSource(new NamesSourceFunction());
        DataStream<RoutableMessage> namesIngress = names.map(name -> RoutableMessageBuilder.builder()
                .withTargetAddress(DEVICE, name)
                .withMessageBody(TypedValue.newBuilder()
                        .setValue(ByteString.copyFrom(name, StandardCharsets.UTF_8))
                        .setHasValue(true)
                        .setTypename("example/Name")
                        .build()
                )
                .withMessageBody(name)
                .build());
        StatefulFunctionDataStreamBuilder.builder("example")
                .withDataStreamAsIngress(namesIngress)
                .withRequestReplyRemoteFunction(
                        requestReplyFunctionBuilder(DEVICE, URI.create("http://localhost:8080/statefun"))
                )
                .withFunctionProvider(LOCAL_GREET, type -> new LocalGreet())
                .withConfiguration(statefunConfig)
                .build(env);

        env.execute("Flink Streaming Java API Skeleton");
    }

    @Slf4j
    private static class LocalGreet implements StatefulFunction {

        @Override
        public void invoke(Context context, Object input) {
            log.info("Hello, {}!", input);
        }
    }

    private static class NamesSourceFunction implements SourceFunction<String> {
        private static final long serialVersionUID = 1;

        private volatile boolean canceled;

        @Override
        public void run(SourceContext<String> ctx) throws InterruptedException {
            String[] names = {"Stephan", "Igal", "Gordon", "Seth", "Marta"};
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (true) {
                int index = random.nextInt(names.length);
                final String name = names[index];
                synchronized (ctx.getCheckpointLock()) {
                    if (canceled) {
                        return;
                    }
                    ctx.collect(name);
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }
}

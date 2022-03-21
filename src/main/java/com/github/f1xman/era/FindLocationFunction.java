package com.github.f1xman.era;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

@Slf4j
class FindLocationFunction extends ProcessWindowFunction<DeviceRegisteredEvent, Location, String, GlobalWindow> {

    @Override
    public void process(String key,
                        ProcessWindowFunction<DeviceRegisteredEvent, Location, String, GlobalWindow>.Context context,
                        Iterable<DeviceRegisteredEvent> iterable,
                        Collector<Location> collector) {
        List<DeviceRegisteredEvent.Station> stations = StreamSupport.stream(iterable.spliterator(), false)
                .map(DeviceRegisteredEvent::getStation)
                .collect(toList());
        log.info("Triangulating location using the following stations: {}", stations);
        DeviceRegisteredEvent.Station station = stations.get(stations.size() - 1);
        Location location = new Location(station.getLatitude(), station.getLongitude());
        collector.collect(location);
    }
}

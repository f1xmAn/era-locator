package com.github.f1xman.era;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

import static lombok.AccessLevel.PRIVATE;

@ToString
@Getter
@FieldDefaults(level = PRIVATE, makeFinal = true)
@RequiredArgsConstructor(onConstructor_ = {@JsonCreator})
public class Location {

    @JsonProperty("latitude")
    double latitude;
    @JsonProperty("longitude")
    double longitude;

}

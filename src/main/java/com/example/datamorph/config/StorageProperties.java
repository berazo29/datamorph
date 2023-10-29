package com.example.datamorph.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class StorageProperties {
    @Value("${storage.write}")
    private String writeLocation;

    @Value("${storage.read}")
    private String readLocation;
}
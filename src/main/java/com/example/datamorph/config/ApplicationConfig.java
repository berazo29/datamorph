package com.example.datamorph.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ApplicationConfig {
    @Bean
    @ConditionalOnProperty(name = "service.spark.mode", havingValue = "local")
    public SparkSession sparkSessionLocal() {
        log.info("Starting default Spark Session.");
        return SparkSession.builder().master("local").appName("APP SPARK").getOrCreate();
    }

    @Bean
    @ConditionalOnProperty(name = "service.spark.mode", havingValue = "yarn")
    public SparkSession sparkSessionYarn() {
        log.info("Starting Spark Session with Yarn.");
        return SparkSession.builder().master("yarn").appName("APP SPARK").getOrCreate();
    }
}

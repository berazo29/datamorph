package com.example.datamorph;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ApplicationConfig {
    @Bean
    public SparkSession sparkSession() {
        log.info("Starting local Spark Session");
        return SparkSession.builder().master("local[4]").appName("APP SPARK").getOrCreate();
    }
}

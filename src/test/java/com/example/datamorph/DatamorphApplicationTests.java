package com.example.datamorph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.io.File;
import java.util.Properties;

@SpringBootApplication
class DatamorphApplicationTests {

    @TempDir
    File tmpDir;

    @Test
    void flightsContextLoads() {
        Properties properties = new Properties();
        properties.put("storage.write", tmpDir.getAbsoluteFile());
        new SpringApplicationBuilder(DatamorphApplication.class)
                .properties(properties).run("flightsWithHeader.csv", "flight");
    }

    @Test
    void fooContextLoads() {
        Properties properties = new Properties();
        properties.put("storage.write", tmpDir.getAbsoluteFile());
        new SpringApplicationBuilder(DatamorphApplication.class)
                .properties(properties).run("fooWithHeader.csv", "foo");
    }
}

package com.example.datamorph;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class Writer {
    @Value("${storage.write}")
    private String writeLocation;

    public void writeCsv(final Dataset<Row> df, final String filename) {
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("_yyyy-MM-dd_HH:mm:ss");
        final LocalDateTime now = LocalDateTime.now();
        final String outFilename = Path.of(writeLocation, filename + dtf.format(now)).toString();
        df.write().csv(outFilename);
    }
}

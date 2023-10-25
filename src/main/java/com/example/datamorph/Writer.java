package com.example.datamorph;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.col;

@Component
public class Writer {
    @Value("${storage.write}")
    private String writeLocation;

    public void writeCsv(final Dataset<Row> df, final String filename) {
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("_yyyy-MM-dd_HH:mm:ss");
        final LocalDateTime now = LocalDateTime.now();
        final String outFilename = Path.of(writeLocation, filename + dtf.format(now)).toString();
        df.select(col("_corrupt_record")).write().csv(outFilename + "_corrupted");
        df.where(col("_corrupt_record").isNull()).drop("_corrupt_record").write().csv(outFilename);
    }
}

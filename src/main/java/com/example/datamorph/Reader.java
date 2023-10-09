package com.example.datamorph;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Component
@Setter
@Getter
public class Reader {
    @Value("${storage.read}")
    private String readLocation;
    SparkSession sparkSession;
    private String filename;

    public Reader(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> getDataframeHeaderOnFlatCsv() {
        return sparkSession.read().csv(Path.of(readLocation, filename).toString());
    }
}

package com.example.datamorph;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@Slf4j
@SpringBootApplication
public class DatamorphApplication {

    @Autowired
    static Reader reader;
    @Autowired
    static Writer writer;

    public static void main(final String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(DatamorphApplication.class, args);
        log.info("APPLICATION FINISHED");
        log.info("Application Started !!");
        final String filename = Arrays.stream(args).findFirst().orElseThrow();
        log.info("Process [filename={}].", filename);
        final Dataset<Row> df = reader.getCsvDataframe(filename);
        final String outFilesDirPath = writer.writeCsv(df, filename);
        log.info("Spark Partition information. [partition(s)={}]", df.toJavaRDD().getNumPartitions());
        log.info("Spark Partition files output at: {}", outFilesDirPath);
    }
}

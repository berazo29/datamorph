package com.example.datamorph;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class DatamorphApplication implements CommandLineRunner {

    @Autowired
    Reader reader;
    @Autowired
    Writer writer;

    private static final Logger logger = LoggerFactory.getLogger(DatamorphApplication.class);

    public static void main(final String[] args) {
        logger.info("STARTING THE APPLICATION");
        SpringApplication.run(DatamorphApplication.class, args);
        logger.info("APPLICATION FINISHED");
    }

    @Override
    public void run(final String... args) throws Exception {
        logger.info("Application Started !!");
        final String filename = Arrays.stream(args).findFirst().get();
        reader.setFilename(args[0]);
        logger.info("Process filename: {}", filename);
        final Dataset<Row> df = reader.getDataframeHeaderOnFlatCsv();
        df.show(10);
        logger.info("Number of partitions: " + df.toJavaRDD().getNumPartitions());
        writer.writeCsv(df, reader.getFilename());
    }
}

package com.example.datamorph;

import com.example.datamorph.models.FileInformation;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@Slf4j
@SpringBootApplication
public class DatamorphApplication implements CommandLineRunner {

    @Autowired
    Reader reader;
    @Autowired
    Writer writer;

    @Autowired
    private BeanFactory beanFactory;

    public static void main(final String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(DatamorphApplication.class, args);
        log.info("APPLICATION FINISHED");
    }

    @Override
    public void run(final String... args) {
        log.info("Application Started !!");
        final String filename = Arrays.stream(args).findFirst().orElseThrow();
        final String schemaName = Arrays.stream(args).skip(1).findFirst().orElseThrow();
        FileInformation fileInformation = beanFactory.getBean(FileInformation.class, filename, schemaName);
        log.info("Process [{}].", fileInformation);
        final Dataset<Row> df = reader.getCsvDataframe(fileInformation);
        final String outFilesDirPath = writer.writeCsv(df, fileInformation.getFilename());
        log.info("Spark Partition information. [partition(s)={}]", df.toJavaRDD().getNumPartitions());
        log.info("Spark Partition files output at: {}", outFilesDirPath);
    }
}

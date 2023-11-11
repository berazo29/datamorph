package com.example.datamorph.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class ReaderProperties {
    @Value("${reader.corrupt-records.enabled}")
    private Boolean corruptRecordsEnabled;

    @Value("${reader.corrupt-records.name:_corrupt_record}")
    private String corruptedColumnName;

    @Value("${reader.schema.enabled}")
    private Boolean schemaEnabled;

    @Value("${reader.schema.print-enabled}")
    private Boolean schemaPrintEnabled;

    @Value("${reader.header-enabled}")
    private Boolean headerEnabled;

    @Value("${reader.spark-mode}")
    private String sparkMode;
}

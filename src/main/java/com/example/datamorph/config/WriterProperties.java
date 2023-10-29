package com.example.datamorph.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class WriterProperties {
    @Value("${writer.corrupt-records.write}")
    private Boolean writeCorruptRecordsEnabled;

    @Value("${writer.corrupt-records.header-enabled}")
    private Boolean corruptRecordsHeaderEnabled;

    @Value("${writer.header-enabled}")
    private Boolean headerEnabled;

    @Value("${writer.corrupt-records.postfix:_corrupt_records}")
    private String corruptedRecordsDirnamePostfix;

    @Value("${writer.postfix-date.enabled}")
    private Boolean postfixDateEnabled;
}

package com.example.datamorph;

import com.example.datamorph.config.ReaderProperties;
import com.example.datamorph.config.StorageProperties;
import com.example.datamorph.config.WriterProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.col;

@Slf4j
@Component
@RequiredArgsConstructor
public class Writer {

    final WriterProperties writerProperties;
    final ReaderProperties readerProperties;

    final StorageProperties storageProperties;

    public String writeCsv(Dataset<Row> df, final String filename) {
        final String writeLocation = storageProperties.getWriteLocation();
        final Boolean writeCorruptRecordsEnabled = writerProperties.getWriteCorruptRecordsEnabled();
        final Boolean readerCorruptRecordsEnabled = readerProperties.getCorruptRecordsEnabled();
        final Boolean headerOn = writerProperties.getHeaderEnabled();
        final Boolean corruptRecordsHeaderEnabled = writerProperties.getCorruptRecordsHeaderEnabled();
        String outFilename = filename;
        if (writerProperties.getPostfixDateEnabled()) {
            final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("_yyyy-MM-dd_HH:mm:ss");
            final LocalDateTime now = LocalDateTime.now();
            outFilename = filename + dtf.format(now);
        }
        outFilename = Path.of(writeLocation, outFilename).toString();
        log.info("Writing records at: {}", outFilename);

        if (writeCorruptRecordsEnabled) {
            if (readerCorruptRecordsEnabled) {
                final String corruptRecord = readerProperties.getCorruptedColumnName();
                final String malformedRecordsDirPath = outFilename + writerProperties.getCorruptedRecordsDirnamePostfix();
                df.select(col(corruptRecord)).write().option("header", corruptRecordsHeaderEnabled).csv(malformedRecordsDirPath);
                df = df.where(col(corruptRecord).isNull()).drop(corruptRecord);
                log.info("Malformed records write location at: {}", malformedRecordsDirPath);
            } else {
                log.warn("Set the reader property [reader.corrupt-records=true] to enable the writer of corrupt records. Default behaviour is to skip writing of corrupt records.");
            }
        }
        df.write().option("header", headerOn).csv(outFilename);
        return outFilename;
    }
}

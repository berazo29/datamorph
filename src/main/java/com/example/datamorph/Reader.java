package com.example.datamorph;

import com.example.datamorph.config.ReaderProperties;
import com.example.datamorph.config.StorageProperties;
import com.example.datamorph.models.FileInformation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Slf4j
@Component
@RequiredArgsConstructor
public class Reader {
    final ReaderProperties readerProperties;
    final StorageProperties storageProperties;
    final SchemaResolver schemaResolver;
    final SparkSession sparkSession;

    public Dataset<Row> getCsvDataframe(FileInformation fileInformation) {
        final Boolean readCorruptRecordsEnabled = readerProperties.getCorruptRecordsEnabled();
        final Boolean headerEnabled = readerProperties.getHeaderEnabled();
        final String readLocation = storageProperties.getReadLocation();
        final Boolean schemaPrintEnabled = readerProperties.getSchemaPrintEnabled();
        final StructType schema = schemaResolver.getResolveSchema(fileInformation.getSchemaName());

        DataFrameReader dataFrameReader = getSparkMode();

        if (schema != null) {
            dataFrameReader = dataFrameReader
                    .schema(schema);
        }

        if (readCorruptRecordsEnabled) {
            dataFrameReader = dataFrameReader
                    .option("columnNameOfCorruptRecord", readerProperties.getCorruptedColumnName());
        }

        log.warn("Property set [headerEnabled = `{}`]", headerEnabled);
        dataFrameReader = dataFrameReader.option("header", headerEnabled);

        final Dataset<Row> df;

        df = dataFrameReader.csv(Path.of(readLocation, fileInformation.getFilename()).toString()).cache();
        if (schemaPrintEnabled) {
            df.printSchema();
        }
        return df;
    }

    public DataFrameReader getSparkMode() {
        DataFrameReader dataFrameReader = sparkSession.read();
        final String sparkMode = readerProperties.getSparkMode();
        switch (sparkMode) {
            case "FAILFAST":
                dataFrameReader = dataFrameReader.option("mode", "FAILFAST");
                break;
            case "DROPMALFORMED":
                dataFrameReader = dataFrameReader.option("mode", "DROPMALFORMED");
                break;
            case "PERMISSIVE":
                dataFrameReader = dataFrameReader.option("mode", "PERMISSIVE");
                break;
            default:
                log.warn("[reader.spark-mode=`{}`]. Set to default value `PERMISSIVE`.", sparkMode);
                break;
        }
        return dataFrameReader;
    }
}

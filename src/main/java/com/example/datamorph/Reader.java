package com.example.datamorph;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.List;

@Component
@Setter
@Getter
public class Reader {
    @Value("${storage.read}")
    private String readLocation;
    SparkSession sparkSession;
    private String filename;

    private StructType schema;

    public Reader(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> getDataframeHeaderOnFlatCsv() {

        // for simplicity is using schema for badFoo.csv and foo.csv
        if (schema == null) {
            schema = DataTypes.createStructType(
                    List.of(
                            DataTypes.createStructField("Name", DataTypes.StringType, false),
                            DataTypes.createStructField("Age", DataTypes.IntegerType, false),
                            DataTypes.createStructField("Alive", DataTypes.BooleanType, false),
                            DataTypes.createStructField("_corrupt_record", DataTypes.StringType, true)
                    ));
        }
        final Dataset<Row> df = sparkSession.read()
                .option("header", true)
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .schema(schema)
                .csv(Path.of(readLocation, filename).toString()).cache();
        df.printSchema();
        return df;
    }
}

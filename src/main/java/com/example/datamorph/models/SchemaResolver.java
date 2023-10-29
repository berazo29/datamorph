package com.example.datamorph.models;

import com.example.datamorph.config.ReaderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class SchemaResolver {

    final ReaderProperties readerProperties;

    public StructType getResolveSchema() {
        final Boolean schemaEnabled = readerProperties.getSchemaEnabled();
        final Boolean corruptRecordsEnabled = readerProperties.getCorruptRecordsEnabled();
        final String schemaName = readerProperties.getSchemaName();
        final List<String> supportedSchemas = List.of("flight", "foo");


        if (corruptRecordsEnabled && !schemaEnabled) {
            log.error("Set the property configuration requirement [reader.corrupt-records-enabled=true] and [reader.schema.enabled=true]. Given [corruptRecordsEnabled={}, schemaEnabled={}].", corruptRecordsEnabled, schemaEnabled);
            throw new RuntimeException("Misconfiguration corruptRecordsEnabled and schemaEnabled");
        }
        if (!schemaEnabled) {
            log.warn("No schema provided. Supported schemas = {}. More Information under `MySchemas.class`.", supportedSchemas);
            return null;
        }
        final StructType schema;
        switch (schemaName) {
            case "flight":
                schema = getFlightSchema();
                break;
            case "foo":
                schema = getFooSchema();
                break;
            default:
                log.error("[schema=`{}`] not found. Supported schemas = {}. More Information under `MySchemas.class`.", schemaName, supportedSchemas);
                throw new RuntimeException("schema=`" + schemaName + "` not found.");
        }
        return schema;
    }

    private StructType getFooSchema() {
        final MySchemas mySchemas = new MySchemas();
        if (readerProperties.getCorruptRecordsEnabled()) {
            return mySchemas.getFooSchema().add(getDefaultCorruptedField());
        }
        return mySchemas.getFooSchema();
    }

    private StructType getFlightSchema() {
        final MySchemas mySchemas = new MySchemas();
        if (readerProperties.getCorruptRecordsEnabled()) {
            return mySchemas.getFlightSchema().add(getDefaultCorruptedField());
        }
        return mySchemas.getFlightSchema();
    }

    private StructField getDefaultCorruptedField() {
        return DataTypes.createStructField(readerProperties.getCorruptedColumnName(), DataTypes.StringType, true);
    }
}

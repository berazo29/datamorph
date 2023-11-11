package com.example.datamorph;

import com.example.datamorph.config.ReaderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

import static com.example.datamorph.models.MySchemas.*;


@Component
@Slf4j
@RequiredArgsConstructor
public class SchemaResolver {

    final ReaderProperties readerProperties;

    public StructType getResolveSchema(String schemaName) {
        final Boolean schemaEnabled = readerProperties.getSchemaEnabled();
        final Boolean corruptRecordsEnabled = readerProperties.getCorruptRecordsEnabled();

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
        if (readerProperties.getCorruptRecordsEnabled()) {
            return fooSchema.add(getDefaultCorruptedField());
        }
        return fooSchema;
    }

    private StructType getFlightSchema() {
        if (readerProperties.getCorruptRecordsEnabled()) {
            return flightSchema.add(getDefaultCorruptedField());
        }
        return flightSchema;
    }

    private StructField getDefaultCorruptedField() {
        return DataTypes.createStructField(readerProperties.getCorruptedColumnName(), DataTypes.StringType, true);
    }
}

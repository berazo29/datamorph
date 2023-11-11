package com.example.datamorph.models;

import lombok.Data;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import java.util.List;

@Data
public class MySchemas {
    public final static List<String> supportedSchemas = List.of("flight", "foo");

    public final static StructType fooSchema = DataTypes.createStructType(
            List.of(
                    DataTypes.createStructField("Name", DataTypes.StringType, false, Metadata.empty()),
                    DataTypes.createStructField("Age", DataTypes.IntegerType, false, Metadata.empty()),
                    DataTypes.createStructField("Alive", DataTypes.BooleanType, false, Metadata.empty())
            ));
    public final static StructType flightSchema = DataTypes.createStructType(
            List.of(
                    DataTypes.createStructField("YEAR", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("MONTH", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DAY_OF_WEEK", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("AIRLINE", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("FLIGHT_NUMBER", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("TAIL_NUMBER", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("ORIGIN_AIRPORT", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("DESTINATION_AIRPORT", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("SCHEDULED_DEPARTURE", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DEPARTURE_TIME", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DEPARTURE_DELAY", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("TAXI_OUT", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("WHEELS_OFF", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("SCHEDULED_TIME", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("ELAPSED_TIME", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("AIR_TIME", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DISTANCE", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("WHEELS_ON", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("TAXI_IN", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("SCHEDULED_ARRIVAL", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("ARRIVAL_TIME", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("ARRIVAL_DELAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("DIVERTED", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("CANCELLED", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("CANCELLATION_REASON", DataTypes.StringType, true, Metadata.empty()),
                    DataTypes.createStructField("AIR_SYSTEM_DELAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("SECURITY_DELAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("AIRLINE_DELAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("LATE_AIRCRAFT_DELAY", DataTypes.IntegerType, true, Metadata.empty()),
                    DataTypes.createStructField("WEATHER_DELAY", DataTypes.IntegerType, true, Metadata.empty())
            ));

}

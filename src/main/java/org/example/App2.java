package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class App2 {
    public static void main(String[] args) {
        // Set Spark log level to ERROR
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentStreaming")
                .master("local[2]") // Use "yarn" for deployment on a YARN cluster
                .getOrCreate();

        // Set application log level to INFO
        Logger.getLogger("org.example").setLevel(Level.INFO);



        Dataset<Row> dataset = spark.read()
                .option("header",true)
                .csv("ints.csv");

        dataset.show();

        //  Display the number of incidents per service
        Dataset<Row> incidentsByService = dataset.groupBy("service")
                .agg(count("Id").alias("incident_count"));

        incidentsByService.show();

        // Task 2: Display the two years with the highest number of incidents
        Dataset<Row> incidentsByYear = dataset.withColumn("year", substring(col("date"), 1, 4))
                .groupBy("year")
                .agg(count("Id").alias("incident_count"))
                .orderBy(col("incident_count").desc())
                .limit(2);

        incidentsByYear.show();

    }
}

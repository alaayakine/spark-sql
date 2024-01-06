package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class App1 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentStreaming")
                .master("local[2]") // Use "yarn" for deployment on a YARN cluster
                .getOrCreate();
        Logger.getLogger("org.example").setLevel(Level.INFO);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "******");

        Dataset<Row> datasetC=spark.read().option("header",true).jdbc("jdbc:mysql://localhost:3306/db_hopital,","consultations",connectionProperties);
        Dataset<Row> datasetM=spark.read().option("header",true).jdbc("jdbc:mysql://localhost:3306/db_hopital,","medecins",connectionProperties);
        Dataset<Row> datasetP=spark.read().option("header",true).jdbc("jdbc:mysql://localhost:3306/db_hopital,","patients",connectionProperties);

        datasetC.show();

        Dataset<Row> dateNaissance = datasetC.groupBy("date_consultation").agg(count("*").as("le nombre de consultations par jour"));

        dateNaissance.show();

        Dataset<Row> join = datasetC.join(datasetM,datasetC.col("id_medecin").equalTo(datasetM.col("id"))).select(col("nom"),col("prenom")).groupBy(col("nom"),col("prenom")).agg(count("*").as("NOMBRE DE CONSULTATION"));
        join.show();
        Dataset<Row> join1 = datasetC.join(datasetM,datasetC.col("id_medecin").equalTo(datasetM.col("id")));
        Dataset<Row> join2 = join1.join(datasetP, join1.col("id_patient").equalTo(datasetP.col("id"))).select(join1.col("nom"),join1.col("prenom"));
        Dataset<Row> Np = join2.select(col("nom"), col("prenom")).groupBy(col("nom"),col("prenom")).agg(count("*").as("nombre de patients"));
        Np.show();


    }
}
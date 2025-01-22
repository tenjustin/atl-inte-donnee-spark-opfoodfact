package foodproject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder()
            .master("local[6]")
            .appName("OpenFoodFact Data")
            .config("spark.sql.warehouse.dir", "file:/workspace")
            .getOrCreate().newSession();

        try {
            Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", "\t")
                .option("mode", "DROPMALFORMED") // Ignore les lignes mal formées
                .load("/workspaces/atl-integration-donnee/en.openfoodfacts.org.products.csv");

            df.show(10); 

        } catch (Exception e) {
            System.err.println("Erreur lors de la lecture du fichier CSV: " + e.getMessage());
            e.printStackTrace();
        } finally {
            sparkSession.close();
        }
    }
}

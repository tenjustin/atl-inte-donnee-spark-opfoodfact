package foodproject;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {
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
          .load("/workspaces/atl-integration-donnee/en.openfoodfacts.org.products.csv")
          .select("product_name", "brands", "countries", "ingredients_text", "energy_100g",
              "proteins_100g", "carbohydrates_100g", "fat_100g", "labels", "packaging");

      Dataset<Row> dataset = df.select(
          df.col("product_name"),
          df.col("brands"),
          df.col("countries"),
          df.col("ingredients_text"),
          df.col("energy_100g").cast("float"),
          df.col("proteins_100g").cast("float"),
          df.col("carbohydrates_100g").cast("float"),
          df.col("fat_100g").cast("float"),
          df.col("labels"),
          df.col("packaging")
      );

      System.out.println("Nombre de lignes : " + dataset.count());
      System.out.println("Colonnes : " + Arrays.toString(dataset.columns()));
      System.out.println("Types de données : " + Arrays.toString(dataset.dtypes()));

      dataset.show(10);
      dataset.printSchema();

      // Filtrage des lignes inutilisables
      Dataset<Row> dfFiltered = dataset
          .filter(
              "product_name IS NOT NULL AND countries IS NOT NULL AND energy_100g IS NOT NULL AND proteins_100g IS NOT NULL "
                  +
                  "AND carbohydrates_100g IS NOT NULL AND fat_100g IS NOT NULL AND labels IS NOT NULL");
      
      // Uniformisation des champs texte du dataset
      Dataset<Row> uniformDf = dfFiltered.withColumn("countries", lower(col("countries")))
          .withColumn("brands", lower(col("brands")))
          .withColumn("product_name", lower(col("product_name")))
          .withColumn("ingredients_text", lower(col("ingredients_text")))
          .withColumn("labels", lower(col("labels")))
          .withColumn("packaging", lower(col("packaging")));

      Dataset<Row> dfCleaned = uniformDf.dropDuplicates();

      dfCleaned.show(10);
      dfCleaned.describe().show();

    } catch (Exception e) {
      System.err.println("Erreur lors de la lecture du fichier CSV: " + e.getMessage());
      e.printStackTrace();
    } finally {
      sparkSession.close();
    }
  }
}

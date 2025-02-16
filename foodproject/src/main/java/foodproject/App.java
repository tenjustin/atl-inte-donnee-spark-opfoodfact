package foodproject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
      Dataset<Row> df = OFFDataExtractor.loadOFFCsv(sparkSession, 
        "/workspaces/atl-integration-donnee/en.openfoodfacts.org.products.csv");

      Dataset<Row> dfCleaned = OFFDataCleaner.cleanOFFDataset(df);

      Dataset<Row> dfTransformed = OFFDataTransformer.transformOFFDataset(dfCleaned);

      OFFAgreggator.agreggateOFFDataset(dfCleaned);

      OFFExporter.exportTransformedDataset(dfTransformed, "/workspaces/atl-integration-donnee/output/transformed.csv");

    } 
    catch (Exception e) {
      System.err.println("Erreur lors de la lecture du fichier CSV: " + e.getMessage());
      e.printStackTrace();
    } 
    finally {
      sparkSession.close();
    }
  }
}

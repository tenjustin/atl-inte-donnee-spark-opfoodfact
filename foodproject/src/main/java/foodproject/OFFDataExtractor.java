package foodproject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OFFDataExtractor {
    public static Dataset<Row> loadOFFCsv(SparkSession sparkSession, String path) {
        Dataset<Row> df = sparkSession.read()
            .format("csv")
            .option("header", "true")
            .option("delimiter", "\t")
            .option("mode", "DROPMALFORMED")
            .load("/workspaces/atl-integration-donnee/en.openfoodfacts.org.products.csv")
            .select("product_name", "brands", "countries", "ingredients_text", "energy_100g",
                "proteins_100g", "carbohydrates_100g", "fat_100g", "labels", "packaging", "sugars_100g");

        return formatOFFDataset(df);
    }

    private static Dataset<Row> formatOFFDataset(Dataset<Row> df) {
        return df.select(
            df.col("product_name"),
            df.col("brands"),
            df.col("countries"),
            df.col("ingredients_text"),
            df.col("energy_100g").cast("float"),
            df.col("proteins_100g").cast("float"),
            df.col("carbohydrates_100g").cast("float"),
            df.col("fat_100g").cast("float"),
            df.col("sugars_100g").cast("float"),
            df.col("labels"),
            df.col("packaging")
        );
    }

}

package foodproject;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OFFDataCleaner {
    public static Dataset<Row> cleanOFFDataset(Dataset<Row> df){
        df = filterUnusableRows(df);
        df = formatOFFDataset(df);
        df = df.dropDuplicates();
        return df;
    }

    private static Dataset<Row> filterUnusableRows(Dataset<Row> df){
        return df.filter(
            "product_name IS NOT NULL AND countries IS NOT NULL AND energy_100g IS NOT NULL AND proteins_100g IS NOT NULL "
                +
                "AND carbohydrates_100g IS NOT NULL AND fat_100g IS NOT NULL AND sugars_100g IS NOT NULL AND labels IS NOT NULL");
    }

    private static Dataset<Row> formatOFFDataset(Dataset<Row> df) {
        return df.withColumn("countries", lower(col("countries")))
          .withColumn("brands", lower(col("brands")))
          .withColumn("product_name", lower(col("product_name")))
          .withColumn("ingredients_text", lower(col("ingredients_text")))
          .withColumn("labels", lower(col("labels")))
          .withColumn("packaging", lower(col("packaging")));
    }
}

package foodproject;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OFFDataTransformer {
    public static Dataset<Row> transformOFFDataset(Dataset<Row> df) {
        df = df.withColumn("is_healthy",
                col("energy_100g").lt(250)
                        .and(col("fat_100g").lt(10))
                        .and(col("carbohydrates_100g").lt(20))
                        .and(col("proteins_100g").gt(5)))
                .withColumn("ingredient_count",
                        size(split(col("ingredients_text"), ",")))
                .withColumn("countries_array", split(col("countries"), ","))
                .filter(array_contains(col("countries_array"), "france"))
                .drop("countries_array");

        return df;
    }
}

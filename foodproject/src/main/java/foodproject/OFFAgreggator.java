package foodproject;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OFFAgreggator {
    public static void agreggateOFFDataset(Dataset<Row> df) {
        df = df.withColumn("countries_array", split(col("countries"), ","));
        Dataset<Row> top10BrandsDf = top10BrandsAggregation(df);
        Dataset<Row> avgSugarEnergyByCountryDf = avgSugarEnergyByCountryAggregation(df);
        Dataset<Row> productDistributionByLabelsDf = productDistributionByLabelsAggregation(df);
        writeAggregation(top10BrandsDf, "/workspaces/atl-integration-donnee/output/topBrands.csv");
        writeAggregation(avgSugarEnergyByCountryDf, "/workspaces/atl-integration-donnee/output/avgSugarEnergyByCountry.csv");
        writeAggregation(productDistributionByLabelsDf, "/workspaces/atl-integration-donnee/output/productDistributionByLabels.csv");
    }

    private static void writeAggregation(Dataset<Row> df, String path) {
        df.coalesce(1).write()
                .format("csv")
                .option("header", "true")
                .save(path);
    }

    private static Dataset<Row> top10BrandsAggregation(Dataset<Row> df) {
        return df.groupBy("brands")
                .count()
                .orderBy(col("count").desc())
                .limit(10);
    }

    private static Dataset<Row> avgSugarEnergyByCountryAggregation(Dataset<Row> df) {
        return df
                .withColumn("country", explode(col("countries_array")))
                .groupBy("country")
                .agg(
                        avg("sugars_100g").alias("avg_sugars_100g"),
                        avg("energy_100g").alias("avg_energy_100g"));
    }

    private static Dataset<Row> productDistributionByLabelsAggregation(Dataset<Row> df) {
        return df.groupBy("labels")
                .count()
                .orderBy(col("count").desc());
    }
}

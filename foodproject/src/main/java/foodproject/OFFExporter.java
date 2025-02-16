package foodproject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OFFExporter {
    public static void exportTransformedDataset(Dataset<Row> df, String path) {
        df.coalesce(1).write()
                .format("csv")
                .option("header", "true")
                .save(path);
    }
}

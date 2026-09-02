import delta
import utils
import tqdm
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
class Ingestor:
    def __init__(self, spark, catalog, schemaname, tablename, data_format):
        self.catalog    = catalog
        self.spark      = spark
        self.schemaname = schemaname
        self.tablename  = tablename
        self.format     = data_format
        self.set_schema()

    def set_schema(self):
        self.data_schema = utils.import_schema(self.tablename)
    
    def load(self, path):
        df = (self.spark.read
                .format(self.format)
                .option("sep", ";")
                .option("header", True)
                .schema(self.data_schema)
                .load(path))
        return df
    
    def save(self, df):
        (df.coalesce(1).write.format("delta").mode("overwrite").saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}"))
    
    def execute(self, path):
        df = self.load(path)
        return self.save(df)


class IngestorCDC(Ingestor):
    def __init__(self, spark, catalog, schemaname, tablename, data_format, id_field, timestamp_field):
        super().__init__(spark, catalog, schemaname, tablename, data_format)
        self.id_field        = id_field
        self.timestamp_field = timestamp_field
        self.set_deltatable()
    
    def set_deltatable(self):
        tablename = f"{self.catalog}.{self.schemaname}.{self.tablename}"
        self.deltatable = delta.DeltaTable.forName(self.spark, tablename)

    def upsert(self, df):
        if self.timestamp_field:
            window_spec = Window.partitionBy(self.id_field).orderBy(
                col(self.timestamp_field).desc()
            )
            df_cdc = (df.withColumn("row_number", row_number().over(window_spec))
                        .filter(col("row_number") == 1)
                        .drop("row_number"))
        else:
            df_cdc = df.dropDuplicates([self.id_field])

        (self.deltatable.alias("b")
         .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    def load(self, path):
        df = (self.spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("sep", ";")
                .option("header", True)
                .schema(self.data_schema)
                .load(path))
        
        return df
    
    def save(self, df):
        stream = (df.writeStream
          .option("checkpointLocation", f"/Volumes/workspace/{self.schemaname}/cdc/checkpoint_{self.tablename}/")
          .foreachBatch(lambda df, batch_id: self.upsert(df))
          .trigger(availableNow=True))

        return stream
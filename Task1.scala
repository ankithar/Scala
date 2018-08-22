import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object Task1 {
  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf()
    //conf.setAppName("Datasets Test")
    //conf.setMaster("local[2]")
    //val sc = new SparkContext(conf)

    val folder = System.getProperty("user.dir")

    val spark: SparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", folder)
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local").appName("Test1").getOrCreate
    val sc = spark.sparkContext // Just used to create test RDDs

    val inputFile = args(0)
    val df1 = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(inputFile)
     // .load("E:\\USC\\DataMining\\Assignment\\Assignment1\\Assignment_01\\ml-latest-small\\ml-latest-small\\ratings.csv")

    val DF = df1.select("movieId","rating")
    // DF.show()

    val result = DF.groupBy("movieId")
      .agg(avg("rating").as("rating_avg"))
      .sort(asc("movieId"))

    //result.show()

    val outputFolder = args(1)
    //val outputFolder ="C:\\Users\\ankit\\Desktop\\tmp\\result119"
    result.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputFolder)
  }
}

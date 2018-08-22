import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
object Task2 {
  def main(args: Array[String]): Unit = {

    val folder = System.getProperty("user.dir")

    val spark: SparkSession = SparkSession.builder()
      .config("spark.sql.warehouse.dir", folder)
     // .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .master("local").appName("Test2").getOrCreate
    val sc = spark.sparkContext // Just used to create test RDDs

    val inputFile1 = args(0)

    // Read the CSV file
    //val ratings = sc.textFile("E:\\USC\\DataMining\\Assignment\\Assignment1\\Assignment_01\\ml-latest-small\\ml-latest-small\\ratings.csv")
    val ratings = sc.textFile(inputFile1)
    // split / clean data
    val headerAndRows1 = ratings.map(line => line.split(",").map(_.trim))
    // get header
    val header1 = headerAndRows1.first
    //print(header.deep.mkString("\n"))
    // filter out header
    val dataRatings = headerAndRows1.filter(_(0) != header1(0))
    //data.collect().foreach(arr => println(arr.mkString(", ")))
    //val data2Ratings = dataRatings.map(arr => (arr(1).toInt,arr(2).toFloat))
    val data2Ratings = dataRatings.map(arr => (arr(1).toInt,arr(2).toFloat))

    val dfWithSchemaRatings = spark.createDataFrame(data2Ratings).toDF("movie_id", "ratings")
    dfWithSchemaRatings.show()


    val inputFile2 = args(1)
    // Read the CSV file
    val tags = sc.textFile(inputFile2)
    // split / clean data
    val headerAndRows2 = tags.map(line => line.split(",").map(_.trim))
    // get header
    val header2 = headerAndRows2.first
    //print(header.deep.mkString("\n"))
    // filter out header
    val dataTags = headerAndRows2.filter(_(0) != header2(0))
    //data.collect().foreach(arr => println(arr.mkString(", ")))
    val datatags2 = dataTags.map(arr => (arr(2),arr(1).toInt))
    val dfWithSchemaTags = spark.createDataFrame(datatags2).toDF("tag", "movie_id")
    dfWithSchemaTags.show()

    val DF = dfWithSchemaRatings
      .join(dfWithSchemaTags,"movie_id")
      .groupBy("tag")
      .agg(avg("ratings").as("average_rating"))
      .sort(desc("tag"))

    val outputFolder =args(2)
    DF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputFolder)

  }
}

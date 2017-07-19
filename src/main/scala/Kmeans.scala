import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by i1551 on 7/12/17.
  */
object Kmeans {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("K Means").config(conf).getOrCreate()
    case class Record(dt:LocalDate, lat: Double, lon: Double, base: String)

    val schema = StructType(Array(
      StructField("dt", TimestampType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("base", StringType, true)
    ))
    val df = spark.read
      .option("header","false")
      .option("inferSchema", true)
        .schema(schema)
      .csv("./Data/data.csv")

    df.printSchema()
    df.show()

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    val df2 = assembler.transform(df)

    df2.show()
    
      val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)
      
      val kmeans = new KMeans().setK(8).setFeaturesCol("features").setPredictionCol("prediction")
      val model = kmeans.fit(df2)
      println("Final Centers: ")
      model.clusterCenters.foreach(println)

      val categories = model.transform(testData)
    println("Categories.show: ")
      categories.show
      
         categories.createOrReplaceTempView("uber")

    import spark.implicits._

    println("Which hours of the day and which cluster had the highest number of pickups? ")
    categories.select(month($"dt").alias("month"), dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"prediction")
      //How many pickups occurred in each cluster?
      .groupBy("month", "day", "hour", "prediction")
      .agg(count("prediction").alias("count")).orderBy("day", "hour", "prediction").show

    categories.select(hour($"dt").alias("hour"), $"prediction").groupBy("hour", "prediction").agg(count("prediction")
      .alias("count")).orderBy(desc("count")).show

    categories.groupBy("prediction").count().show()

    spark.sql("select prediction, count(prediction) as count from uber group by prediction").show

    spark.sql("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show

    /*
     * uncomment below for various functionality:
    */
    // to save the model 
    model.write.overwrite().save("/user/user01/data/savemodel")
    // model can be  re-loaded like this
    // val sameModel = KMeansModel.load("/user/user01/data/savemodel")
    //  
    // to save the categories dataframe as json data
    val res = spark.sql("select dt, lat, lon, base, prediction as cid FROM uber order by dt")   
    res.write.format("json").save("/user/user01/data/uber.json")

  }

  }

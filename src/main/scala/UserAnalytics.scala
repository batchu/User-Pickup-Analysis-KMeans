import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object UserAnalytics {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    case class User(id: String, age: String, gender: String, profession: String, zipcode: String)

    val data: RDD[User] = sc.textFile("./Data/u.user") //Location of the data file
      .map(line => line.split(","))
      .map(userRecord => User(userRecord(0),
        userRecord(1), userRecord(2), userRecord(3), userRecord(4)))


    val uniqueProfessions = data.map(u => u.profession)
      .distinct()
      .count()
//    print(uniqueProfessions)

    val usersByZipCode = data.map(u=> (u.zipcode, 1)).reduceByKey(_ + _ ).sortBy(-_._2)
    usersByZipCode.collect.foreach(println)


  }

}

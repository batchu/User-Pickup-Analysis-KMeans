import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Zepplin {

  def main(args:Array[String])={

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("K Means").config(conf).getOrCreate()

    val bankText = sc.textFile(("./Data/bank/bank-full.csv"))

    case class Bank(age: Integer, job: String, marital: String, education: String, balance:Integer)

    val bank = bankText.map(s=>s.split(";")).filter(s=>s(0)!="\"age\"").map(
      s=>Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt
      )
    )

    import spark.implicits._
//    bank.toDF().createOrReplaceTempView("bank")


  }
}

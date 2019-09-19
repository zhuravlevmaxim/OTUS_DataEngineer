package otus_data_engineer.json_reader_zhuravlev

import org.apache.spark.sql.SparkSession


object JsonReader extends App {

  case class Wine(id: BigInt, country: String, points: BigInt, title: String, variety: String, winery: String)

  val spark = SparkSession.builder().appName("JsonReader").getOrCreate()
  import spark.implicits._

  val sparkContext = spark.sparkContext
  val pathToFileJSon = args(0)
  val wine_df = spark.read.json(pathToFileJSon).as[Wine]
  wine_df.collect().foreach(w => println(w.id, w.country, w.points, w.title, w.variety, w.winery))
}

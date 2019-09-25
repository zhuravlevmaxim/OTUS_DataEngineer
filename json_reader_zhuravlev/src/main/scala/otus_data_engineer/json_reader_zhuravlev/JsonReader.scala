package otus_data_engineer.json_reader_zhuravlev

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._


object JsonReader extends App {

  case class Wine(
                   id: Option[BigInt], country: Option[String],
                  points: Option[BigInt], title: Option[String],
                   variety: Option[String], winery: Option[String]
                 )

  val spark = SparkSession.builder().appName("JsonReader").master("local[*]").getOrCreate()

  val sparkContext = spark.sparkContext
  val pathToFileJSon = args(0)
  val jsonLikeString = spark.sparkContext.textFile(pathToFileJSon)

  protected implicit val jsonFormats: Formats = DefaultFormats

  jsonLikeString
    .map(wineStr => parse(wineStr).extract[Wine])
    .foreach(wine => println(
        wine.id.getOrElse(None),
        wine.country.getOrElse("empty country"),
        wine.points.getOrElse(None),
        wine.title.getOrElse("empty title"),
        wine.variety.getOrElse("empty variety"),
        wine.winery.getOrElse("empty winery")
    )
  )
}

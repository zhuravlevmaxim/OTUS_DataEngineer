package otus_data_engineer.lesson7_hw

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import otus_data_engineer.lesson7_hw.case_classes.{Crime, OffenseCodes}

object BostonCrimesMap extends App {

  val spark = SparkSession.builder().appName("BostonCrimesMap").getOrCreate()
  import spark.implicits._

  val paths = args.take(3)
  val pathToCrimeFile = paths(0)
  val pathToOffenseCodesFile = paths(1)
  val pathToSaveResultFile = paths(2)

  val crimeSchema = Encoders.product[Crime].schema
  val crimeDf = spark.read
    .option("header", "true").option("sep", ",").option("encoding", "utf-8")
    .schema(crimeSchema)
    .csv(pathToCrimeFile).as[Crime]

  val offenseCodesSchema = Encoders.product[OffenseCodes].schema
  val offenseCodesDf = spark.read
    .option("header", "true").option("sep", ",").option("encoding", "utf-8")
    .schema(offenseCodesSchema)
    .csv(pathToOffenseCodesFile).distinct().as[OffenseCodes]

  val offenseCodesClearDf = offenseCodesDf.map(offence => (offence.CODE, offence.NAME.get.split("-")(0).trim()))
    .toDF("CODE", "NAME")
  val offenseCodesBroadcastDf = broadcast(offenseCodesClearDf)
  val crimeOffenceCodesJoin = crimeDf.join(offenseCodesBroadcastDf,
    $"CODE" === $"OFFENSE_CODE", "INNER")
  crimeOffenceCodesJoin.createOrReplaceTempView("crimeOffenceCodesJoin")
  crimeOffenceCodesJoin.persist()

    spark.sql(
      """
        |SELECT
        |     t1.DISTRICT,
        |     COUNT(*) AS crimes_total,
        |     CEIL(COUNT(*) / 12) AS crimes_monthly,
        |     AVG(t1.LAT) AS lat,
        |     AVG(t1.LONG) AS lng
        |FROM crimeOffenceCodesJoin AS t1
        |GROUP BY t1.DISTRICT
        |ORDER BY t1.DISTRICT DESC
      """.stripMargin).createOrReplaceTempView("preResultTable")

  val separateForCrimeTypes = ", "
  spark.sql(
    s"""
      |SELECT t2.DISTRICT, CONCAT_WS('$separateForCrimeTypes', COLLECT_LIST(t2.NAME)) AS frequent_crime_types
      |FROM(
      |      SELECT t3.DISTRICT, t3.NAME, COUNT(*) AS counts
      |      FROM crimeOffenceCodesJoin AS t3
      |      GROUP BY t3.DISTRICT, t3.NAME
      |      ORDER BY t3.DISTRICT DESC, counts DESC
      |      ) AS t2
      |GROUP BY t2.DISTRICT
      |ORDER BY t2.DISTRICT DESC
    """.stripMargin).map(row => (row.getString(0),
                         row.getString(1).split(separateForCrimeTypes).take(3).mkString(separateForCrimeTypes)))
        .toDF("DISTRICT", "frequent_crime_types").createOrReplaceTempView("districtAndCrimeTypes")

  val resultTable = spark.sql(
    """
      |SELECT t1.DISTRICT, t1.crimes_total, t1.crimes_monthly, t2.frequent_crime_types, t1.lat, t1.lng
      |FROM preResultTable AS t1
      |INNER JOIN districtAndCrimeTypes AS t2
      |ON t1.DISTRICT = t2.DISTRICT
    """.stripMargin)
  resultTable.repartition(1).write.format("parquet").save(pathToSaveResultFile)
}

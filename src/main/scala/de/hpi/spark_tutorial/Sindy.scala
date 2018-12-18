package de.hpi.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession, DataFrame}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
      // collect_set eliminates duplicates from set
      // size returns length of array or set
      // explode creates new row for each element in given array or map column


      import spark.implicits._

      val resources = inputs
          .foldLeft(Set[DataFrame]())((acc, resource) => {
            acc + spark.read
              .option("inferSchema", "true")
              .option("header", "true")
              .option("quote", "\"")
              .option("delimiter", ";")
              .csv(resource)
          })



    val region = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(inputs(0))

    region.show()

    val nation = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(inputs(1))

    nation.show()

    val x : Int = nation


    val scemas = resources
      .foreach


    val scema_region = region.schema.fieldNames

    val result0 : RDD[(String, Set[String])] = region
      .flatMap(row => row.toSeq.view.zipWithIndex.foldLeft(Seq[(String, Set[String])]())((a, b) => {
        a :+ (b._1.toString(), Set(scema_region(b._2)))
      }))
      .rdd
      .reduceByKey(_ ++ _)

    // result0.foreach(println)

    val scema_nation = nation.schema.fieldNames

    val result : RDD[(String, Set[String])] = nation
      .flatMap(row => row.toSeq.view.zipWithIndex.foldLeft(Seq[(String, Set[String])]())((a, b) => {
        a :+ (b._1.toString(), Set(scema_nation(b._2)))
      }))
      .rdd
      .reduceByKey(_ ++ _)
        .union(result0)

    // result.foreach(println)

    println("----------------------------------------------------------------------")

    var tmp : RDD[Set[String]] = result
        .groupByKey()
        .map(row => row._2.foldLeft(Set[String]())((acc, b) => {
          acc ++ b
      }))
        .distinct()

    tmp.foreach(println)





  }
}

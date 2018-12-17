package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
      // collect_set eliminates duplicates from set
      // size returns length of array or set
      // explode creates new row for each element in given array or map column


      import spark.implicits._

      val region = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(inputs(0)) // also text, json, jdbc, parquet
        // .as[String]// String)]

      /*
      try to infer type
      val nation = spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv(inputs(1))
          // .as[(Int, String, Int, String)]
      */

       val nation2 = spark
         .read
         .option("inferSchema", "false")
         .option("header", "true")
         .option("quote", "\"")
         .option("delimiter", ";")
         .csv(inputs(1))
         .toDF("nation_key", "nation_name", "region_key", "comment")
         .as[(String, String, String, String)]

      region.show(10)
      nation2.show(10)

      println("--------------")

      // val test = nation.flatMap(row => row.split(" "))

      val result = nation2
          .map(line => line)
          // .repartition(32)
          // .flatMap(line => line.split(";"))
          // .map(line => line)
    // .flatMap((key:String, name:String, reg:String, cmd:String) => List(key, name, reg, cmd))
        // .map((key, name:String, reg:String, cmd:String):String  => "") // convert to dataset
        // .flatMap(line => line)

      result.show(10)

      println(result.getClass())

  }
}

package de.hpi.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
      // collect_set eliminates duplicates from set
      // size returns length of array or set
      // explode creates new row for each element in given array or map column


      import spark.implicits._

      /*
      val region = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(inputs(0)) // also text, json, jdbc, parquet
        // .as[String]// String)]

      try to infer type
      val nation = spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv(inputs(1))
          // .as[(Int, String, Int, String)]
      */

/*
      val region = spark
        .read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(inputs(0))
        .toDF("region_key", "region_name", "comment1")
        .as[(String, String, String)] // do not use Int, because you might get cast err

       val nation = spark
         .read
         .option("inferSchema", "false")
         .option("header", "true")
         .option("quote", "\"")
         .option("delimiter", ";")
         .csv(inputs(1))
         .toDF("nation_key", "nation_name", "region_key", "comment2")
         .as[(String, String, String, String)]

      nation.show(10)
      region.show(10)
      */



    val region = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(inputs(0))

    println(region)

    val nation = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(inputs(1))

    println(nation)



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

    // result.foreach(println)


    println("----------------------------------------------------------------------------------------")

    val result1 : RDD[(String, Set[String])] = result0
      .union(result)
        .reduceByKey(_ ++ _)

    result1.foreach(println)




/*

    val result2 : RDD[(String, Set[String])] = nation2
      .flatMap(row => row.toString().filterNot("[]".toSet).split(",").view.zipWithIndex.foldLeft(Seq[(String, Set[String])]())((a, b) => {
        println(b._2)
        a :+ (b._1.toString(), Set(scema(b._2)))
      }))
      .rdd
      .reduceByKey(_ ++ _)

//    result2.foreach(println)


    println("\n---------------------------------------------------------------------------------------------------\n")

    val result  : RDD[(String, Set[String])] = nation
      .flatMap(row => Seq((row._1, Set(scema(0))), (row._2, Set(scema(1))), (row._3, Set(scema(2))), (row._4, Set(scema(3)))))
      .rdd
      .reduceByKey(_ ++ _)  // RDD[(String, Set[String])]

    // result.foreach(println)

*/
    /*
      // works, but hard coded
      val result  : RDD[(String, Set[String])] = nation
          .flatMap(row =>  Seq((row._1, Set("nation_key")), (row._2, Set("nation_name")), (row._3, Set("region_key")), (row._4, Set("comment2"))))
          .rdd
          .reduceByKey(_ ++ _)  // RDD[(String, Set[String])]
*/


    // result.foreach(println(_))

    // val tmp : Int  =  result // check type





        /*
          .map(row => {
            // each ele in row with head of row
            ((row(0), "region_key"), (row(1), "region_name"), (row(2), "comment1"),
            (row(3), "nation_key"), (row(4), "nation_name"), (row(5), "region_key"),
            (row(6), "comment2"))
          })
          .reduce(row => 1)
*/
          //.join(region,
            //usingColumns = Seq("region_key", "region_name", "comment1", "nation_key", "nation_name", "comment2"),  "full_outer")
          // .repartition(32)
          // .flatMap(line => line  .split(";"))
          // .map(line => line)
    // .flatMap((key:String, name:String, reg:String, cmd:String) => List(key, name, reg, cmd))
        // .map((key, name:String, reg:String, cmd:String):String  => "") // convert to dataset
        // .flatMap(line => line)

//      println(result.getClass())

  }
}

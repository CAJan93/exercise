package de.hpi.spark_tutorial

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, RelationalGroupedDataset, Row, SparkSession, DataFrame}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // collect_set eliminates duplicates from set
    // size returns length of array or set
    // explode creates new row for each element in given array or map column


    import spark.implicits._


    // read all inputs
    val resources = inputs
      .foldLeft(List[DataFrame]())((acc, resource) => {
        acc :+ spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .option("quote", "\"")
          .option("delimiter", ";")
          .csv(resource)
      })


    // create all scemas of the inputs
    val scemas = resources
      .foldLeft(List[Array[String]]())((acc, resource) => {
        acc :+ resource.schema.fieldNames
      })


    // over all inputs
    val result0: List[RDD[(String, Set[String])]] = resources
      .view
      .zipWithIndex
      .foldLeft(List[RDD[(String, Set[String])]]())((acc, resource) => {
        acc :+ resource._1
          .flatMap(row => row.toSeq.view.zipWithIndex.foldLeft(Seq[(String, Set[String])]())((a, b) => {
            a :+ (b._1.toString(), Set(scemas(resource._2)(b._2))) // get the column names
          }))
          .rdd
          .reduceByKey(_ ++ _)
      })


    // union everything
    val result1: RDD[(String, Set[String])] = result0
      .foldLeft(RDD[(String, Set[String])]())((acc, resource) => {
        acc.union(resource)
      })


    result1.foreach(println)


    println("----------------------------------------------------------------------")
  }
}

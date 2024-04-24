package com.ysw.spark.test

import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDemo")
      .getOrCreate()

    import sparkSession.implicits._
    val value = sparkSession.createDataset(Seq(1, 2, 3))

    value.show
  }
}

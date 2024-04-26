package com.ysw.spark.test

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 测试ClickHouse的DataSourceV2实现
 */
object CKTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CKTest").getOrCreate();
    val df = spark.read
      .format("clickhouse")
      .option("url", "jdbc:clickhouse://10.0.10.140:8123,10.0.10.141:8123,10.0.10.141:18123,10.0.10.141:28123,10.0.10.142:8123,10.0.10.142:18123,10.0.10.142:28123/?socket_timeout=60000")
      .option("user", "default")
      .option("password", "jxcluster")
      .option("database", "nx_data")
      .option("table", "(select start_time,msisdn,imsi,imei,cgi from dec_location_cdr_local where toYYYYMMDD(start_time) = '20240425')")
      .option("use_time_zone", "Asia/Shanghai")
      .load()
    println(df.count())

    //    import spark.implicits._
    //    df.where($"id" === 328).distinct().coalesce(1).write
    //      .format("clickhouse")
    //      .option("url", "jdbc:clickhouse://10.0.5.30:9000/")
    //      .option("user", "default")
    //      .option("password", "EverWH135@#$%")
    //      .option("cluster", "everdc_ck_cluster")
    //      .option("table", "dec_call_log_test_local")
    //      .option("use_server_time_zone", "false")
    //      .option("use_time_zone", "Asia/Shanghai")
    //      .option("max_memory_usage", "2000000000")
    //      .option("max_bytes_before_external_group_by", "1000000000")
    //      .mode(SaveMode.Append)
    //      .save();
    spark.close()
  }
}








package spark_work

import org.apache.spark.sql.SparkSession
import spark_work.SparkWorkers.spark

object SparkWorkers {

  val spark = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[*]")
    .enableHiveSupport() //开启spark对hive支持
    .getOrCreate()

}

class SparkWorkers {

  def work01(args: String): String = {
    //获得对应表的连接，并注册为临时表
    spark.table("pinjiaman_cash.phone_info").createOrReplaceTempView("phone_info")
    spark.table("pinjiaman_cash.customer_order").createOrReplaceTempView("customer_order")
    //sql查询
    val sql_1 =
      s"""
         |select
         |customer_id,deviced_id,add_time
         |from phone_info
         |where customer_id in ($args)
      """.stripMargin
    //    (select customer_id from customer_order)
    val df_SQL_imei_all = spark.sql(sql_1) //.dropDuplicates("customer_id", "deviced_id")
    //注册为临时表
    df_SQL_imei_all.createOrReplaceTempView("SQL_imei_all")
    //sql查询
    val sql_2 =
      """
        |select
        |customer_id,deviced_id,add_time
        |from phone_info
        |where deviced_id in
        |(select deviced_id from SQL_imei_all)
      """.stripMargin
    val df = spark.sql(sql_2)
    //结果展示
    df.printSchema()
    df.count().toString
  }

}



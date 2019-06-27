package spark_work

import java.util.Properties

import org.apache.spark.sql.SparkSession

object sparkonmysql {

  def main(args: Array[String]): Unit = {
    //获得SparkSession客户端连接对象
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("spark://c01:7077") //"local[*]"
      .getOrCreate()
    //设置SparkSql连接mysql配置
    val conn = new Properties()
    conn.setProperty("user", "jiazhimiao")
    conn.setProperty("password", "gj/fYlJAZ3V0xqFXT0p43g==")
    conn.setProperty("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://149.129.219.85:3306/data"
    //获得对应表的连接，并注册为临时表
    val df_phone_info = spark.read.jdbc(url, "phone_info", conn)
    val df_customer_order = spark.read.jdbc(url, "customer_order", conn)
    df_phone_info.createTempView("phone_info")
    df_customer_order.createTempView("customer_order")
    val df1 = spark.sql("select * from phone_info limit 10 ")
    //sql查询，并去重
    val sql_1 = "select " +
      "customer_id,deviced_id,add_time " +
      "from phone_info " +
      "where customer_id in " +
      "(select customer_id from customer_order)"
    val df_SQL_imei_all = spark.sql(sql_1) //.dropDuplicates("customer_id", "deviced_id")
    //注册为临时表
    df_SQL_imei_all.createTempView("SQL_imei_all")
    //sql查询
    val sql_2 = "select " +
      "customer_id,deviced_id,add_time " +
      "from phone_info " +
      "where deviced_id in " +
      "(select deviced_id from SQL_imei_all)"
    val df = spark.sql(sql_2)
    //结果展示
    df.write.json("/tmp/mytmp/")
  }

}
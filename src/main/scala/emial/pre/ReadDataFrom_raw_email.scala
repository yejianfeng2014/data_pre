package emial.pre

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ReadDataFrom_raw_email {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url_target = "jdbc:mysql://127.0.0.1:3306/local_first_2"


    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)
    //todo:4、显示mysql中表的数据


    mysqlDF.write.mode(SaveMode.Overwrite).jdbc(url_target, "bt_paypal_dispute_info", properties)


    spark.stop()


  }

}

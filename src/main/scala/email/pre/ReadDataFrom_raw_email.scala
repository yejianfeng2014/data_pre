package email.pre

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


// TODO:  读取和写入的路径需要进一步确认

// 拉去10w 的数据量过来看看
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

    val mysqlDF_inbox: DataFrame = spark.read.jdbc(url, "bt_email_inbox", properties)


    val mysqlDF_inbox_content: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content", properties)


    mysqlDF_inbox.createOrReplaceTempView("t")

    mysqlDF_inbox_content.createOrReplaceTempView("c")


    // 内存撑不住

//    val sql = " SELECT inbox.message_id, inbox.SUBJECT , content.text_html from inbox,content where inbox.email_id = content.email_id limit 1000  "

//    val value = mysqlDF_inbox_content.select("")

//    val frame_query_10000 = spark.sql(sql)


    // 采用左连接 读取

// todo 内存依然撑不住，数据大概只有15000左右
    val  sql_new = " SELECT   t.message_id AS message_id,   t. SUBJECT AS SUBJECT,   c.text_plain AS text_plain,   c.text_html AS text_html FROM   c LEFT JOIN  t ON t.email_id = c.email_id WHERE   t.create_time > 1551369600 AND t. SUBJECT IN (   'Order tracking',   'Size doesn’t fit',   'Not as described/Quality problem',   'Payment issue',   'Return&Exchange',   'Non-receipt order',   'Refund issue',   'Others' )"


    val frame_test = spark.sql(sql_new)

    frame_test.write.mode(SaveMode.Overwrite).jdbc(url_target, "bt_email_101", properties)


    spark.stop()


  }

}

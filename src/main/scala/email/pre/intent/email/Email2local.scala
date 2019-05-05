package email.pre.intent.email
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 把测试数据库的数据移动到本地的数据库
  */

// 采用本地跑，跑完把结果移动过去。10w级别的数据移动很快
object Email2local {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("language_dec")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "12354")
    val url = "jdbc:mysql://172.21.1.91:3306/ai_data?useUnicode=yes&characterEncoding=UTF-8"


    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_ai_email", properties)


    val properties_local: Properties = new Properties()
    properties_local.setProperty("user", "demo")
    properties_local.setProperty("password", "123456")

    val url_local = "jdbc:mysql://127.0.0.1:3306/local_data_tongji"

    mysqlDF.write.mode(SaveMode.Overwrite).jdbc(url_local, "bt_ai_email", properties_local)


    spark.stop()

  }


}

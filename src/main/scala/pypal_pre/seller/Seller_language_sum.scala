package pypal_pre.seller

import java.io.File
import java.util.Properties

import com.cybozu.labs.langdetect.DetectorFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import pypal_pre.Language_dect

object Seller_language_sum {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]") // 这个需要单核跑，多核载入语言包出了问题
      .appName("seller language detect")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_seller_all_says", properties)


    val uri = Language_dect.getClass.getResource("/profiles").toURI()

    DetectorFactory.loadProfile(new File(uri))

    def getStringLanguage(str_my: String): String = {

      val detector = DetectorFactory.create()

      detector.append(str_my)
      try {
        val str = detector.detect()

        str
      } catch {
        case e: Exception => ""
      }
    }


    val my_udf_2 = udf(getStringLanguage _) //将自定义函数注册为udf

    val out = mysqlDF.withColumn("messages_1", my_udf_2($"seller_message_1")) //使用udf进行转换操作


    out.write.mode(SaveMode.Overwrite).jdbc(url, "bt_paypal_dispute_info_seller_all_says_dec", properties)

    spark.stop()

  }


}

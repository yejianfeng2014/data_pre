package email.pre

import java.util.Properties

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  *
  * 数据预处理，按照英文常见的预处理方法处理数据
  *
  * 1，把错误的数据过滤掉
  * 2，分词
  * 3，去掉停用词
  * 4，词形还原
  * 5，词干化
  *
  * 6，做一个英文和数字想换转换的字典，这个是初期的使用，后期打算使用goggle 词向量，再后期自己训练词向量
  *
  */
object ParseData2int {


  def main(args: Array[String]): Unit = {


//    val spark = SparkSession
//      .builder()
//      .master("local[*]")
//      .appName("buyer first dialogue")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()
//
//    val properties: Properties = new Properties()
//    properties.setProperty("user", "demo")
//    properties.setProperty("password", "123456")
//
//    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
//
//    val mysqlDF_inbox: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_4", properties)
//
//
//    val filter_string_error = mysqlDF_inbox.filter(!mysqlDF_inbox("parsed_html_lower") === "error")
//
//
//    println(filter_string_error.count())


    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点

    val rdd=sc.textFile("D:\\data_pre\\data\\emails\\test.txt")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.top(50).foreach(println)


}


}

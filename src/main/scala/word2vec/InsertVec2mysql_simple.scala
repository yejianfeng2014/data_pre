package word2vec

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  *
  *
  * 把词向量存入数据
  * 数据格式，单词，单词300维度的数据
  */

// TODO:  case class 不能超过354 列 
object InsertVec2mysql_simple {

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

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"


    import spark.implicits._
    val file_path = "file:\\D:\\python_workspace\\semanaly\\input_glove\\glove.840B.300d.txt"

    val frame_rdd = spark.read.text(path = file_path)


    val frame_cased = frame_rdd.map(a => a.getString(0).split(" ")).map(a =>
      Word(a.head,
        a.tail.mkString(",")

      )
    )


    //    value.show()

    //    println(frame.count())

    //    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)


    frame_cased.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "glove_840B_300d", properties)


    spark.stop()


  }


  case class Word(word: String,
                  col_1: String
                 )


}

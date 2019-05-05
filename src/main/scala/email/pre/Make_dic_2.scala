package email.pre

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements


/**
  * 制作单词，编号表
  *
  * 制作编号，单词表
  *
  *
  */
object Make_dic_2 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .config("spark.some.config.option", "some-value")
      .appName("buyer first dialogue")
      .getOrCreate()

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    //    val url = "jdbc:mysql://127.0.0.1:3306/email_webmail_one"
    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"

    // todo 修改读的邮件表的内容

    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_5", properties)


//    mysqlDF.show(100)
    val value_content = mysqlDF.select("content")

//     value.show(10)

//  value.rdd.flatMap(a =>a.toString().split(";")).map(a =>(a,1)).reduceByKey(_+_)


    val result_value = value_content.rdd.flatMap(a =>a.toString().split(";")).map(a =>(a,1))

     val result_value_1 =  result_value.map(a =>{
       val a_1 = if (a._1.contains("[")) a._1.replace("[","") else ""

       val a_2 = if (a._1.contains("]")) a._1.replace("]","") else  ""

       val catcont = a_1 + a_2


        (catcont.trim,a._2)

      }).reduceByKey(_+_)


    val value = result_value_1.map(a =>Row(a._1,a._2))



    // 2.定义schema，带有StructType的
    // 定义schema信息
    val struct = StructType(
      Array(
        StructField("word",StringType),
        StructField("word_count",IntegerType)

      )
    )

    val word_count = spark.createDataFrame(value, struct)

    // 在原Schema信息的基础上添加一列 “id”信息
    val schema: StructType = word_count.schema.add(StructField("id", LongType))

    // DataFrame转RDD 然后调用 zipWithIndex
    val dfRDD: RDD[(Row, Long)] = word_count.rdd.zipWithIndex()

    val rowRDD: RDD[Row] = dfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))

    // 将添加了索引的RDD 转化为DataFrame
    val df2 = spark.createDataFrame(rowRDD, schema)

//    df2.show()

    // TODO:  这个需要打开 
//    df2.write.mode(saveMode =SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_6",properties)


//    result_value.take(10).foreach(println)

//    print(result_value)

    
    // 把每一个单词转变成数字


    val dic : DataFrame = spark.read.jdbc(url, "bt_email_inbox_content_parsed_html_language_temp_6", properties)


    val frame_word_id = dic.select("word","id")


    val stringToInt = scala.collection.mutable.Map[String, Long]()


    frame_word_id.rdd.collect().foreach(a => stringToInt(a.getString(0))= a.getLong(1))


    println(stringToInt.size)

    def word2id(str: String): String = {

        if (str.contains(";")) {

          try {
            val ints = str.split(";").map(a => stringToInt.getOrElse(a, 0)).mkString(";")

            ints
          } catch {
            case e: Exception => println(str)
              ""
          }

        } else {

          val value = stringToInt.getOrElse(str,"")

          value.toString
        }

    }

    val my_udf = udf(word2id _) //将自定义函数注册为udf

    import  spark.implicits._
    val value_is_not_null = mysqlDF.filter($"content".isNotNull)

    val out = value_is_not_null.withColumn("content_int", my_udf($"content")) //使用udf进行转换操作


    out.write.mode(saveMode =SaveMode.Overwrite).jdbc(url,"bt_email_inbox_content_parsed_html_language_temp_7",properties)





    spark.stop()




  }

}

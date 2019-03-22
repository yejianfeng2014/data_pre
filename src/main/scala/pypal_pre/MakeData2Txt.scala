package pypal_pre

import java.io.FileWriter
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO:   未做呢 

object MakeData2Txt {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[1]") // 采用单核读写，多核并发问题
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first"


    //todo:3、读取mysql中的数据
    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info_pre_2", properties)
    //todo:4、显示mysql中表的数据

    val frame = mysqlDF.select("messages")


    // 统计所有买家的回复的语言

/*

    frame.withColumn()



    val str_3 = "[{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-10-13T07:32:00.000Z\",\"content\":\"I have continually emailed and been very clear on what I want and you have totally disregarded everything I have said. My last e mail said for you to just not to bother sending the other half of my order which is way overdue. I now see today you have dispatched goodness knows what. I have asked for half my money back as a goodwill gesture. I am very dissatisfied with one of the two items I have received and been offered a $4 voucher for berrylook. Which I have declined. I am happy to keep one of the dresses out of the two received and said I don\\u2019t now want the other two, which are now ridiculously late. So 3 out of 4 dresses I do not want. I think half of my purchase price is very fair. I am as I have said disgusted with your service and time keeping. Look at my emails and reply sensibly to what I have asked, not keep asking what I mean. I have been very clear.\"},{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-10-13T07:49:58.000Z\",\"content\":\"I stand corrected you actually offered me a \\u00a35 voucher for berrylook.\"},{\"posted_by\":\"SELLER\",\"time_posted\":\"2018-10-15T09:19:19.000Z\",\"content\":\"E362787\\nDear Customer,\\n\\nSincerely sorry for the inconvenience and delay on the processing time due to the large number of orders we are receiving recently. \\n\\nI have checked your order.  3 items have been sent out by Registered Mail with the tracking NO. UE677033845YP, UE679791534YP. You can check the tracking details by visiting the following link: http:\\/\\/www.17track.net\\/en\\/  \\n\\nThe following is the latest tracking information:\\nUE677033845YP\\n2018-10-09 12:43\\nPortsmouth DO, COLLECTION RECORDED\\n\\nUE679791534YP\\n2018-10-14 21:29\\nITEM ADVISED\\n\\nAs for the item SKU: A2B51CE5BF2C, GBP19.31  , we are sorry that the item  is out of stock due to large demand. There are 3 options for you to choose:\\n1. Please choose another one no more than  GBP 20 to replace and reply to us with the product's SKU, size and color.\\n2.We can offer you a coupon  GBP 20,you can use it next time.\\n3.We can refund to you GBP 19.31 .Please let us know the final idea about this.\\n\\nIf you still have any further inquiries, please reply in this email, and we will always be delighted to provide the best service we can. We appreciate your understanding and support.\"},{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-10-15T17:42:01.000Z\",\"content\":\"You actually need to refund me money please. I no longer want the dress or a voucher to use another time. This is not the outcome I have asked for, in fact you went against what I asked and sent the third item to me when I had given you instruction not to.  But for now I choose option 3. Refund me the money.\"},{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-10-19T17:13:02.000Z\",\"content\":\"I had e mailed a few times prior to coming on here. I have now had no reply to my message I sent through here a couple of days ago. I th8nk you will get the mist through my first e mail.\\nThey were 15 days dispatching two of the four dresses when it was said to be 3-7. I was grossly disappointed with one of them, however chose to keep the other one and asked them if I could return it, to which they said no they would give me a \\u00a35 voucher. I also asked them NOT to send the other two, and they have ignored that and very quickly sent the one that was in stock. This I still haven\\u2019t received. I have in my last messaged asked for a refund of the \\u00a319 they offered. However, I really think I am due at least \\u00a350, but said I would settle at \\u00a340 which is less than half of what I paid them. But I suppose \\u00a319 is better than letting them get away with nothing. So utterly disappointed and feel disgusted that they can get away with this service. Thank you\"},{\"posted_by\":\"BUYER\",\"time_posted\":\"2018-10-31T10:37:02.000Z\",\"content\":\"I have continually emailed and been very clear on what I want and you have totally disregarded everything I have said. My last e mail said for you to just not to bother sending the other half of my order which is way overdue. I now see today you have dispatched goodness knows what. I have asked for half my money back as a goodwill gesture. I am very dissatisfied with one of the two items I have received and been offered a $4 voucher for berrylook. Which I have declined. I am happy to keep one of the dresses out of the two received and said I don't now want the other two, which are now ridiculously late. So 3 out of 4 dresses I do not want. I think half of my purchase price is very fair. I am as I have said disgusted with your service and time keeping. Look at my emails and reply sensibly to what I have asked, not keep asking what I mean. I have been very clear.\"}]"


    val array_3 = JSON.parseArray(str_3)

    println(array_3.size())


    // 解析内容

    for (item <- i until (array_3.size())) {

      //       println(item)

      val nObject = array_3.getJSONObject(item)
      //       val nObject = JSON.parseObject(array_3.getJSONObject(item))
      println(">>>>>>>>>>>>>>>")
      val str = nObject.getString("posted_by")
      val str_2 = nObject.getString("content")

      //       println(item)

      println(str)
      println(str_2)


    }





    val writer = new FileWriter("learningScala.json", true)
    // true 表示追加
    // false 表示覆盖

    for (i <- 0 to 15)
      writer.write(i.toString)


    writer.close()



*/


  }

}

package pypal_pre


import java.io.File

import com.cybozu.labs.langdetect.{Detector, DetectorFactory}

object Language_dect {

  def main(args: Array[String]): Unit = {


     val uri = Language_dect.getClass.getResource("/profiles").toURI()

    DetectorFactory.loadProfile( new File(uri))


    val detector = DetectorFactory.create()

    detector.append("continue claim note")

    val str = detector.detect()

    println(str)

  }

}

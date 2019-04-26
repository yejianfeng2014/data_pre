/*
package word2vec

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


/**
  *
  *
  * 把词向量存入数据
  * 数据格式，单词，单词300维度的数据
  */

// TODO:  case class 不能超过354 列 
object InsertVec2mysql {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("buyer first dialogue")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "demo")
    properties.setProperty("password", "123456")

    val url = "jdbc:mysql://127.0.0.1:3306/local_first_2"


    import spark.implicits._
    val file_path = "file:\\D:\\python_workspace\\semanaly\\input_glove\\glove.840B.300d.txt"

    val frame_rdd = spark.read.text(path = file_path)




    val frame_cased = frame_rdd.map(a => a.getString(0).split(" ")).map(a =>
      Word(a(0),
                a(1).toDouble,
                a(2).toDouble,
                a(3).toDouble,
                a(4).toDouble,
                a(5).toDouble,
                a(6).toDouble,
                a(7).toDouble,
                a(8).toDouble,
                a(9).toDouble,
                a(10).toDouble,
                a(11).toDouble,
                a(12).toDouble,
                a(13).toDouble,
                a(14).toDouble,
                a(15).toDouble,
                a(16).toDouble,
                a(17).toDouble,
                a(18).toDouble,
                a(19).toDouble,
                a(20).toDouble,
                a(21).toDouble,
                a(22).toDouble,
                a(23).toDouble,
                a(24).toDouble,
                a(25).toDouble,
                a(26).toDouble,
                a(27).toDouble,
                a(28).toDouble,
                a(29).toDouble,
                a(30).toDouble,
                a(31).toDouble,
                a(32).toDouble,
                a(33).toDouble,
                a(34).toDouble,
                a(35).toDouble,
                a(36).toDouble,
                a(37).toDouble,
                a(38).toDouble,
                a(39).toDouble,
                a(40).toDouble,
                a(41).toDouble,
                a(42).toDouble,
                a(43).toDouble,
                a(44).toDouble,
                a(45).toDouble,
                a(46).toDouble,
                a(47).toDouble,
                a(48).toDouble,
                a(49).toDouble,
                a(50).toDouble,
                a(51).toDouble,
                a(52).toDouble,
                a(53).toDouble,
                a(54).toDouble,
                a(55).toDouble,
                a(56).toDouble,
                a(57).toDouble,
                a(58).toDouble,
                a(59).toDouble,
                a(60).toDouble,
                a(61).toDouble,
                a(62).toDouble,
                a(63).toDouble,
                a(64).toDouble,
                a(65).toDouble,
                a(66).toDouble,
                a(67).toDouble,
                a(68).toDouble,
                a(69).toDouble,
                a(70).toDouble,
                a(71).toDouble,
                a(72).toDouble,
                a(73).toDouble,
                a(74).toDouble,
                a(75).toDouble,
                a(76).toDouble,
                a(77).toDouble,
                a(78).toDouble,
                a(79).toDouble,
                a(80).toDouble,
                a(81).toDouble,
                a(82).toDouble,
                a(83).toDouble,
                a(84).toDouble,
                a(85).toDouble,
                a(86).toDouble,
                a(87).toDouble,
                a(88).toDouble,
                a(89).toDouble,
                a(90).toDouble,
                a(91).toDouble,
                a(92).toDouble,
                a(93).toDouble,
                a(94).toDouble,
                a(95).toDouble,
                a(96).toDouble,
                a(97).toDouble,
                a(98).toDouble,
                a(99).toDouble,
                a(100).toDouble,
                a(101).toDouble,
                a(102).toDouble,
                a(103).toDouble,
                a(104).toDouble,
                a(105).toDouble,
                a(106).toDouble,
                a(107).toDouble,
                a(108).toDouble,
                a(109).toDouble,
                a(110).toDouble,
                a(111).toDouble,
                a(112).toDouble,
                a(113).toDouble,
                a(114).toDouble,
                a(115).toDouble,
                a(116).toDouble,
                a(117).toDouble,
                a(118).toDouble,
                a(119).toDouble,
                a(120).toDouble,
                a(121).toDouble,
                a(122).toDouble,
                a(123).toDouble,
                a(124).toDouble,
                a(125).toDouble,
                a(126).toDouble,
                a(127).toDouble,
                a(128).toDouble,
                a(129).toDouble,
                a(130).toDouble,
                a(131).toDouble,
                a(132).toDouble,
                a(133).toDouble,
                a(134).toDouble,
                a(135).toDouble,
                a(136).toDouble,
                a(137).toDouble,
                a(138).toDouble,
                a(139).toDouble,
                a(140).toDouble,
                a(141).toDouble,
                a(142).toDouble,
                a(143).toDouble,
                a(144).toDouble,
                a(145).toDouble,
                a(146).toDouble,
                a(147).toDouble,
                a(148).toDouble,
                a(149).toDouble,
                a(150).toDouble,
                a(151).toDouble,
                a(152).toDouble,
                a(153).toDouble,
                a(154).toDouble,
                a(155).toDouble,
                a(156).toDouble,
                a(157).toDouble,
                a(158).toDouble,
                a(159).toDouble,
                a(160).toDouble,
                a(161).toDouble,
                a(162).toDouble,
                a(163).toDouble,
                a(164).toDouble,
                a(165).toDouble,
                a(166).toDouble,
                a(167).toDouble,
                a(168).toDouble,
                a(169).toDouble,
                a(170).toDouble,
                a(171).toDouble,
                a(172).toDouble,
                a(173).toDouble,
                a(174).toDouble,
                a(175).toDouble,
                a(176).toDouble,
                a(177).toDouble,
                a(178).toDouble,
                a(179).toDouble,
                a(180).toDouble,
                a(181).toDouble,
                a(182).toDouble,
                a(183).toDouble,
                a(184).toDouble,
                a(185).toDouble,
                a(186).toDouble,
                a(187).toDouble,
                a(188).toDouble,
                a(189).toDouble,
                a(190).toDouble,
                a(191).toDouble,
                a(192).toDouble,
                a(193).toDouble,
                a(194).toDouble,
                a(195).toDouble,
                a(196).toDouble,
                a(197).toDouble,
                a(198).toDouble,
                a(199).toDouble,
                a(200).toDouble,
                a(201).toDouble,
                a(202).toDouble,
                a(203).toDouble,
                a(204).toDouble,
                a(205).toDouble,
                a(206).toDouble,
                a(207).toDouble,
                a(208).toDouble,
                a(209).toDouble,
                a(210).toDouble,
                a(211).toDouble,
                a(212).toDouble,
                a(213).toDouble,
                a(214).toDouble,
                a(215).toDouble,
                a(216).toDouble,
                a(217).toDouble,
                a(218).toDouble,
                a(219).toDouble,
                a(220).toDouble,
                a(221).toDouble,
                a(222).toDouble,
                a(223).toDouble,
                a(224).toDouble,
                a(225).toDouble,
                a(226).toDouble,
                a(227).toDouble,
                a(228).toDouble,
                a(229).toDouble,
                a(230).toDouble,
                a(231).toDouble,
                a(232).toDouble,
                a(233).toDouble,
                a(234).toDouble,
                a(235).toDouble,
                a(236).toDouble,
                a(237).toDouble,
                a(238).toDouble,
                a(239).toDouble,
                a(240).toDouble,
                a(241).toDouble,
                a(242).toDouble,
                a(243).toDouble,
                a(244).toDouble,
                a(245).toDouble,
                a(246).toDouble,
                a(247).toDouble,
                a(248).toDouble,
                a(249).toDouble,
                a(250).toDouble,
                a(251).toDouble,
                a(252).toDouble,
                a(253).toDouble,
                a(254).toDouble,
                a(255).toDouble,
                a(256).toDouble,
                a(257).toDouble,
                a(258).toDouble,
                a(259).toDouble,
                a(260).toDouble,
                a(261).toDouble,
                a(262).toDouble,
                a(263).toDouble,
                a(264).toDouble,
                a(265).toDouble,
                a(266).toDouble,
                a(267).toDouble,
                a(268).toDouble,
                a(269).toDouble,
                a(270).toDouble,
                a(271).toDouble,
                a(272).toDouble,
                a(273).toDouble,
                a(274).toDouble,
                a(275).toDouble,
                a(276).toDouble,
                a(277).toDouble,
                a(278).toDouble,
                a(279).toDouble,
                a(280).toDouble,
                a(281).toDouble,
                a(282).toDouble,
                a(283).toDouble,
                a(284).toDouble,
                a(285).toDouble,
                a(286).toDouble,
                a(287).toDouble,
                a(288).toDouble,
                a(289).toDouble,
                a(290).toDouble,
                a(291).toDouble,
                a(292).toDouble,
                a(293).toDouble,
                a(294).toDouble,
                a(295).toDouble,
                a(296).toDouble,
                a(297).toDouble,
                a(298).toDouble,
                a(299).toDouble,
        a(300).toDouble
      )
    )


    //    value.show()

    //    println(frame.count())

    //    val mysqlDF: DataFrame = spark.read.jdbc(url, "bt_paypal_dispute_info", properties)


    frame_cased.write.mode(saveMode = SaveMode.Overwrite).jdbc(url, "glove_840B_300d", properties)


    spark.stop()


  }


  case class Word(word: String,
                                    col_1: Double,
                                    col_2: Double,
                                    col_3: Double,
                                    col_4: Double,
                                    col_5: Double,
                                    col_6: Double,
                                    col_7: Double,
                                    col_8: Double,
                                    col_9: Double,
                                    col_10: Double,
                                    col_11: Double,
                                    col_12: Double,
                                    col_13: Double,
                                    col_14: Double,
                                    col_15: Double,
                                    col_16: Double,
                                    col_17: Double,
                                    col_18: Double,
                                    col_19: Double,
                                    col_20: Double,
                                    col_21: Double,
                                    col_22: Double,
                                    col_23: Double,
                                    col_24: Double,
                                    col_25: Double,
                                    col_26: Double,
                                    col_27: Double,
                                    col_28: Double,
                                    col_29: Double,
                                    col_30: Double,
                                    col_31: Double,
                                    col_32: Double,
                                    col_33: Double,
                                    col_34: Double,
                                    col_35: Double,
                                    col_36: Double,
                                    col_37: Double,
                                    col_38: Double,
                                    col_39: Double,
                                    col_40: Double,
                                    col_41: Double,
                                    col_42: Double,
                                    col_43: Double,
                                    col_44: Double,
                                    col_45: Double,
                                    col_46: Double,
                                    col_47: Double,
                                    col_48: Double,
                                    col_49: Double,
                                    col_50: Double,
                                    col_51: Double,
                                    col_52: Double,
                                    col_53: Double,
                                    col_54: Double,
                                    col_55: Double,
                                    col_56: Double,
                                    col_57: Double,
                                    col_58: Double,
                                    col_59: Double,
                                    col_60: Double,
                                    col_61: Double,
                                    col_62: Double,
                                    col_63: Double,
                                    col_64: Double,
                                    col_65: Double,
                                    col_66: Double,
                                    col_67: Double,
                                    col_68: Double,
                                    col_69: Double,
                                    col_70: Double,
                                    col_71: Double,
                                    col_72: Double,
                                    col_73: Double,
                                    col_74: Double,
                                    col_75: Double,
                                    col_76: Double,
                                    col_77: Double,
                                    col_78: Double,
                                    col_79: Double,
                                    col_80: Double,
                                    col_81: Double,
                                    col_82: Double,
                                    col_83: Double,
                                    col_84: Double,
                                    col_85: Double,
                                    col_86: Double,
                                    col_87: Double,
                                    col_88: Double,
                                    col_89: Double,
                                    col_90: Double,
                                    col_91: Double,
                                    col_92: Double,
                                    col_93: Double,
                                    col_94: Double,
                                    col_95: Double,
                                    col_96: Double,
                                    col_97: Double,
                                    col_98: Double,
                                    col_99: Double,
                                    col_100: Double,
                                    col_101: Double,
                                    col_102: Double,
                                    col_103: Double,
                                    col_104: Double,
                                    col_105: Double,
                                    col_106: Double,
                                    col_107: Double,
                                    col_108: Double,
                                    col_109: Double,
                                    col_110: Double,
                                    col_111: Double,
                                    col_112: Double,
                                    col_113: Double,
                                    col_114: Double,
                                    col_115: Double,
                                    col_116: Double,
                                    col_117: Double,
                                    col_118: Double,
                                    col_119: Double,
                                    col_120: Double,
                                    col_121: Double,
                                    col_122: Double,
                                    col_123: Double,
                                    col_124: Double,
                                    col_125: Double,
                                    col_126: Double,
                                    col_127: Double,
                                    col_128: Double,
                                    col_129: Double,
                                    col_130: Double,
                                    col_131: Double,
                                    col_132: Double,
                                    col_133: Double,
                                    col_134: Double,
                                    col_135: Double,
                                    col_136: Double,
                                    col_137: Double,
                                    col_138: Double,
                                    col_139: Double,
                                    col_140: Double,
                                    col_141: Double,
                                    col_142: Double,
                                    col_143: Double,
                                    col_144: Double,
                                    col_145: Double,
                                    col_146: Double,
                                    col_147: Double,
                                    col_148: Double,
                                    col_149: Double,
                                    col_150: Double,
                                    col_151: Double,
                                    col_152: Double,
                                    col_153: Double,
                                    col_154: Double,
                                    col_155: Double,
                                    col_156: Double,
                                    col_157: Double,
                                    col_158: Double,
                                    col_159: Double,
                                    col_160: Double,
                                    col_161: Double,
                                    col_162: Double,
                                    col_163: Double,
                                    col_164: Double,
                                    col_165: Double,
                                    col_166: Double,
                                    col_167: Double,
                                    col_168: Double,
                                    col_169: Double,
                                    col_170: Double,
                                    col_171: Double,
                                    col_172: Double,
                                    col_173: Double,
                                    col_174: Double,
                                    col_175: Double,
                                    col_176: Double,
                                    col_177: Double,
                                    col_178: Double,
                                    col_179: Double,
                                    col_180: Double,
                                    col_181: Double,
                                    col_182: Double,
                                    col_183: Double,
                                    col_184: Double,
                                    col_185: Double,
                                    col_186: Double,
                                    col_187: Double,
                                    col_188: Double,
                                    col_189: Double,
                                    col_190: Double,
                                    col_191: Double,
                                    col_192: Double,
                                    col_193: Double,
                                    col_194: Double,
                                    col_195: Double,
                                    col_196: Double,
                                    col_197: Double,
                                    col_198: Double,
                                    col_199: Double,
                                    col_200: Double,
                                    col_201: Double,
                                    col_202: Double,
                                    col_203: Double,
                                    col_204: Double,
                                    col_205: Double,
                                    col_206: Double,
                                    col_207: Double,
                                    col_208: Double,
                                    col_209: Double,
                                    col_210: Double,
                                    col_211: Double,
                                    col_212: Double,
                                    col_213: Double,
                                    col_214: Double,
                                    col_215: Double,
                                    col_216: Double,
                                    col_217: Double,
                                    col_218: Double,
                                    col_219: Double,
                                    col_220: Double,
                                    col_221: Double,
                                    col_222: Double,
                                    col_223: Double,
                                    col_224: Double,
                                    col_225: Double,
                                    col_226: Double,
                                    col_227: Double,
                                    col_228: Double,
                                    col_229: Double,
                                    col_230: Double,
                                    col_231: Double,
                                    col_232: Double,
                                    col_233: Double,
                                    col_234: Double,
                                    col_235: Double,
                                    col_236: Double,
                                    col_237: Double,
                                    col_238: Double,
                                    col_239: Double,
                                    col_240: Double,
                                    col_241: Double,
                                    col_242: Double,
                                    col_243: Double,
                                    col_244: Double,
                                    col_245: Double,
                                    col_246: Double,
                                    col_247: Double,
                                    col_248: Double,
                                    col_249: Double,
                                    col_250: Double,
                                    col_251: Double,
                                    col_252: Double,
                                    col_253: Double,
                                    col_254: Double,
                                    col_255: Double,
                                    col_256: Double,
                                    col_257: Double,
                                    col_258: Double,
                                    col_259: Double,
                                    col_260: Double,
                                    col_261: Double,
                                    col_262: Double,
                                    col_263: Double,
                                    col_264: Double,
                                    col_265: Double,
                                    col_266: Double,
                                    col_267: Double,
                                    col_268: Double,
                                    col_269: Double,
                                    col_270: Double,
                                    col_271: Double,
                                    col_272: Double,
                                    col_273: Double,
                                    col_274: Double,
                                    col_275: Double,
                                    col_276: Double,
                                    col_277: Double,
                                    col_278: Double,
                                    col_279: Double,
                                    col_280: Double,
                                    col_281: Double,
                                    col_282: Double,
                                    col_283: Double,
                                    col_284: Double,
                                    col_285: Double,
                                    col_286: Double,
                                    col_287: Double,
                                    col_288: Double,
                                    col_289: Double,
                                    col_290: Double,
                                    col_291: Double,
                                    col_292: Double,
                                    col_293: Double,
                                    col_294: Double,
                                    col_295: Double,
                                    col_296: Double,
                                    col_297: Double,
                                    col_298: Double,
                                    col_299: Double,
                                     col_300: Double
                 )






}
*/
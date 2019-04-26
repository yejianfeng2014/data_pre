package com.orderplus;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


public class UserNlp {


    public static void main(String[] args) {
//
       String  ss = "informed seller that item not received and they promised to refund. did not get any refund yet";

//        inf#####_#####med seller #####_##### item not received #####_##### they promised to refund. did not get any refund #####_#####

        String[] sss = SplitSentenceByConj(ss);

    }

    /**
     * 词形还原
     *
     * @param string:字符串
     * @return List<String> 分词、提取词形还原后的结果
     */
    public static List<String> getlema(String text) {
        //单词集合
        List<String> wordslist = new ArrayList<>();
        ;
        //StanfordCoreNLP词形还原
        Properties props = new Properties();  // set up pipeline properties
        props.put("annotators", "tokenize, ssplit, pos, lemma");   //分词、分句、词性标注和次元信息。
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> words = document.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap word_temp : words) {
            for (CoreLabel token : word_temp.get(CoreAnnotations.TokensAnnotation.class)) {
                String lema = token.get(CoreAnnotations.LemmaAnnotation.class);  // 获取对应上面word的词元信息，即我所需要的词形还原后的单词
                wordslist.add(lema);
            }
        }
        return wordslist;
    }


    public static String[] splitSentence(String cmt) {
        /*正则表达式：句子结束符*/
        String regEx = ",|\\.|\\?|!|:|;|~|，|：|。|！|；|？";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(cmt);
        /*按照句子结束符分割句子*/
        String[] words = p.split(cmt);
        /*将句子结束符连接到相应的句子后*/

        return words;
    }

    /**
     * 这个实在句子分割的基础后做的，所以里面没有标点符号，
     *
     * @param text
     * @return
     * 支持单个单词分割
     */
    public static String[] SplitSentenceByConj(String text) {
        String splitLabels = "yet," +
                "but," +
                "so," +
                "and," +
                "or," +
                "neithor," +
                "nor," +
                "when," +
                "while," +
                "as," +
                "whenever," +
                "before," +
                "after," +
                "since," +
                "until," +
                "till," +
                "although," +
                "though," +
                "even though," +
                "while," +
                "however," +
                "that";

        String[] split = splitLabels.split(",");


        String[] s1 = text.split(" ");

        // 每一个单词核对
        for (int i=0 ;i< s1.length;i++) {
            for (String s2 : split) {
                if (s1[i].trim().equals(s2)){
                    s1[i] = "#####_#####";
                }
            }
        }

        String tempString = " ";
        for (String s : s1) {
            tempString = tempString + s +" " ;
        }
        String[] split1 = tempString.split("#####_#####");
        return split1;


    }





}

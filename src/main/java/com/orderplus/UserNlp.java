package com.orderplus;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;



public class UserNlp {



        public static void main(String[] args)  {
            String aString = "jhend925  https://blog.csdn.net/timo1160139211/article/details/77603141. All 2015 GTIs have heated seats, including the S trim level with no options. I used to own a 2013 335i with the news system and now own a 2011 M3 with the stalk system you talked about. The new system on the 2013 was much better. Ok, yeah, you have to turn it on every time, but it's much easier to adjust speed";
            List<String> word = getlema(aString);
            for (int i = 0; i < word.size(); i++) {
                System.out.println(word.get(i));
            }
        }

    /**
     * 词形还原
     * @param string:字符串
     * @return List<String> 分词、提取词形还原后的结果
     * */
    public static List<String> getlema(String text){
        //单词集合
        List<String> wordslist = new ArrayList<>();;
        //StanfordCoreNLP词形还原
        Properties props = new Properties();  // set up pipeline properties
        props.put("annotators", "tokenize, ssplit, pos, lemma");   //分词、分句、词性标注和次元信息。
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation document = new Annotation(text);
        pipeline.annotate(document);
        List<CoreMap> words = document.get(CoreAnnotations.SentencesAnnotation.class);
        for(CoreMap word_temp: words) {
            for (CoreLabel token: word_temp.get(CoreAnnotations.TokensAnnotation.class)) {
                String lema = token.get(CoreAnnotations.LemmaAnnotation.class);  // 获取对应上面word的词元信息，即我所需要的词形还原后的单词
                wordslist.add(lema);
            }
        }
        return wordslist;
    }




    public static String[] splitSentence(String cmt){
        /*正则表达式：句子结束符*/
        String regEx=",|\\.|\\?|!|:|;|~|，|：|。|！|；|？";
        Pattern p =Pattern.compile(regEx);
        Matcher m = p.matcher(cmt);
        /*按照句子结束符分割句子*/
        String[] words = p.split(cmt);
        /*将句子结束符连接到相应的句子后*/
      /*  if(words.length > 0)
        {
            int count = 0;
            while(count < words.length)
            {
                if(m.find())
                {
                    words[count] += m.group();
                }
                count++;
            }
        }*/
        /*输出结果*/
        return words;
    }




}

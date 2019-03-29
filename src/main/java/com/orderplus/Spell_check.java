package com.orderplus;

import com.github.houbb.word.checker.core.impl.EnWordChecker;


/**
 *
 * 效果不太理想而已
 */
public class Spell_check
{


    public static void main(String[] args) {


//        public static void main(String[] args) {
            final String result = EnWordChecker.getInstance().correct("speling");


        boolean correct = EnWordChecker.getInstance().isCorrect("this is my hom");




        System.out.println(correct);

        String correct1 = EnWordChecker.getInstance().correct(" this is my hom");

        System.out.println(correct1);


        System.out.println(result);
//        }

    }
}

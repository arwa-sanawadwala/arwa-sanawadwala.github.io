package com.tw.services

import java.util
import java.util.Random

object ElementsGenerator {

  def getRandomElement (list: util.List[Integer]): Int =
  {
    val rand = new Random
    list.get(rand.nextInt(list.size))
  }

//  def sum(list: List[Int]): Int ={
//    var sum = 0
//    for (d <- list) {
//      sum += d
//    }
//    sum
//  }

//  def randomValues (): Map[Integer,Integer] = {
//    val rand = new Random
//    val elementsList = util.Arrays.asList(100, 1000)
////    val px = Integer.valueOf(rand.nextInt());
////    Map(90 + rand.nextInt((100-98) + 1), getRandomElement(elementsList))
//    Map[Integer,Integer](Integer.valueOf(90 + rand.nextInt((100-98) + 1)),
//      Integer.valueOf(getRandomElement(elementsList)))
//  }



}

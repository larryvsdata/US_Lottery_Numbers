package com.sparkTutorial.myCodes
import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}



object getWinningNumbers {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("lotteryNumbers").setMaster("local[1]")
    val sc = new SparkContext(conf)

    // You may change the file here and make some minor modifications in the code. It would work fine.
    val rawNumbers= sc.textFile("in/Lottery_Pick_10_Winning_Numbers__Beginning_1987.csv")

    //Enter the count you want for most commons
    val N=10

    val getTuples=(line:String) =>{

      val numberList=line.split(" ")
      val pairs = numberList.map(a => (a,1))
      pairs

    }

    val sortGetTuples=(line:String) =>{
      val stringList=line.split(" ")
      val numberList=stringList.map(a=>a.toInt)
      val sortedNumbers=numberList.sorted
      val sortedStrings=sortedNumbers.map(a=>a.toString)



      val uniquePairs = for {
        (x, idxX) <- sortedStrings.zipWithIndex
        (y, idxY) <- sortedStrings.zipWithIndex
        if idxX < idxY
      } yield (x +":"+y)

      uniquePairs
    }

    val sortGetTriples=(line:String) =>{
      val stringList=line.split(" ")
      val numberList=stringList.map(a=>a.toInt)
      val sortedNumbers=numberList.sorted
      val sortedStrings=sortedNumbers.map(a=>a.toString)



      val uniqueTriples = for {
        (x, idxX) <- sortedStrings.zipWithIndex
        (y, idxY) <- sortedStrings.zipWithIndex
        (z, idxZ) <- sortedStrings.zipWithIndex
        if idxX < idxY && idxY < idxZ
      } yield (x +":"+y+":"+z)

      uniqueTriples
    }



    //If you change the file, you have to filter by another word than "Draw"
    val cleanedLines = rawNumbers.filter(line => !line.contains("Draw")).map(line => (line.split(",")(1)))

    // Most common numbers

    println("Most common numbers: ")
    val tuples= cleanedLines.map(line => getTuples(line)).flatMap(x=>x).reduceByKey((x,y)=> x + y)
    val sortedTuples=tuples.sortBy(pair => pair._2, false)
    val topNumbers=sortedTuples.map(pair => pair._1).take(N)

    for (number<- topNumbers) println(number )

    // Most common pairs
    println("Most common pairs: ")
    val pairs= cleanedLines.map(line => sortGetTuples(line)).flatMap(x=>x).map(pair => (pair,1)).reduceByKey((x,y)=> x + y)
    val sortedPairs=pairs.sortBy(pair => pair._2, false)
    val topPairs=sortedPairs.map(pair => pair._1).take(N)
    for (pair<- topPairs) println(pair.split(":")(0), pair.split(":")(1))

    // Most Common Triples
    println("Most common triples: ")
    val triples= cleanedLines.map(line => sortGetTriples(line)).flatMap(x=>x).map(triple => (triple,1)).reduceByKey((x,y)=> x + y)
    val sortedTriples=triples.sortBy(triple => triple._2, false)
    val topTriples=sortedTriples.map(triple=> triple._1).take(N)
    for (triple<- topTriples) println(triple.split(":")(0), triple.split(":")(1), triple.split(":")(2))

  }


  }

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import scala.util.control.Breaks._

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
object DGIMAlgorithm {
  var lim = 1000
  var value =  (math.log(lim)/math.log(2)).toInt
  var window : HashMap[Int, ListBuffer[Int]] = HashMap()
  var numOfBuckets = value + 1
  var listOfBucketSize : ListBuffer[Int] = ListBuffer()
  var t = 0
  var u = 0
  var check = 1
  var actualCount = 0
  var initialCount = 0
  var actualList : Array[String] = new Array[String](lim)


  def func(rdd : RDD[String]): Unit ={
    val rddCollect = rdd.collect()
    //println(rddCollect.length)
    func2(rddCollect)
  }

  def func2(rddCollect : Array[String]): Unit ={
    //println("Array 1 = "+rddCollect(0))
    for(bit <- rddCollect){
      t = (t + 1) % lim
      if(initialCount != 1000){
        initialCount += 1
      }
      for(k <- listOfBucketSize){
        var listOfBucket = window(k)
        //println("listOfBucket = "+listOfBucket)
        //var ind = 0
        //var len = (listOfBucket.size) - 1
        //for(ind <- 0 to len){
        for(lsItem <- listOfBucket){
          //println("ind = "+ind)
          //var lsItem = 999
          //try {
          //   lsItem = listOfBucket(ind)
          //}
          //catch{
          //case iob : IndexOutOfBoundsException => println("list = "+listOfBucket+"   ind = "+ind+" length = "+len)
          //}

          //for(lsItem <- listOfBucket) {
          if (lsItem == t) {
            //println("t = "+t)
            //println("k = "+k)
            //println("lsItem = "+lsItem)
            //println("ind = "+ind)
            //println("listOfBucket "+listOfBucket)
            //window(k).remove(ind)
            window(k) -= lsItem
          }
        }
        var arrIndex = u % lim
        var ch1 = actualList(arrIndex)
        if(ch1 != "999") {
          if (ch1 == "1") {
            if (bit == "0") {
              actualCount -= 1
              actualList(arrIndex) = bit
            }
          }
          else {
            if (bit == "1") {
              actualCount += 1
              actualList(arrIndex) = bit
            }
          }
        }
        else{

          actualList(arrIndex) = bit
          if(bit == "1"){
            actualCount += 1
          }
        }
      }
      u += 1
      if(bit == "1"){
        (window(1)) += t
        for(i <- listOfBucketSize){
          breakable{
            for(lsItem <- window(i)) {

              if (window(i).size > 2) {
                window(i).remove(0)
                var tmp = window(i)(0)
                window(i).remove(0)
                var lastItem = listOfBucketSize(listOfBucketSize.size - 1)
                if (i != lastItem) {
                  var index = 2 * i
                  window(index) += tmp
                }

              }
              else {
                break
              }
            }
          }

        }


      }
    }

    if(initialCount == 1000){
      var ptr = 0
      var numOf1 = 0

      for(key <- listOfBucketSize){
        if(window(key).size > 0){
          ptr = window(key)(0)
        }
      }
      for(key <- listOfBucketSize){
        var itemList = window(key)
        for(i <- itemList){
          if(i == ptr){
            numOf1 += (key * (0.5)).toInt

          }
          else{
            numOf1 += key
          }
        }

      }
      check += 1
      println("Estimated number of ones in the last 1000 bits : " + numOf1)


      println("Actual number of ones in the last 1000 bits :  " + actualCount)
      println()

    }
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Ass5Task2")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    sc.setLogLevel(logLevel="OFF")

    //println("numOfBuckets = "+numOfBuckets)
    var i = 0
    while(i < numOfBuckets){
      var x = (Math.pow(2,i)).toInt
      window += (x -> ListBuffer())
      listOfBucketSize += (x)
      i += 1
    }
    //println(window)
    //println(listOfBucketSize)

    for(j <- 0 to lim-1){
      actualList(j) = "999"
    }

    //println("check = "+actualList(7)+" len = "+actualList.length)




    val words = lines.flatMap(_.split(" "))

    words.foreachRDD(rdd => func(rdd))

    ssc.start()             // Start the computation
    ssc.awaitTermination()
    //ssc.awaitTerminationOrTimeout(120)
    //ssc.stop()
  }

}

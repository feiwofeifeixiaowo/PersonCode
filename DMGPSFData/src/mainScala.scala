/*
 * Created by will on 16-12-16.
 */

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Date}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer


object mainScala {

  def main(args: Array[String]) {

    val startTime = System.nanoTime()//用于记录运行时间
    //    val newDate   = args(0)  //2016-12-02
    val newDate   = args(0)  //2016-12-02
//    val begDate   = args(0)  //20161202
//    val endDate   = args(1)  //20161212
    //FcdBusProxyMessage.2016-12-11.log
    val inFList    = List("2016-12-07","2016-12-08","2016-12-09","2016-12-11")

    // val inFList    = List("2016-12-07","2016-12-08","2016-12-09","2016-12-10","2016-12-11","2016-12-12","2016-12-13")
    val r     = scala.util.Random
    //val newID = idList(r.nextInt(idList.length - 1))
//    val bDate = stringToDate(begDate + "000000")
//    val eDate = stringToDate(endDate + "000000")
//    val result = getDatesBetweenTwoDate(bDate, eDate)

    val conf = new SparkConf().setAppName("FakeGPSData Application")//.set("spark.executor.memory","6g")
    val sc = new SparkContext(conf)

//    for (j <- 1 until(begDate.toInt - endDate.toInt)) {

////      val newDate = result(j - 1)
//      val randomTmp = inFList(r.nextInt(inFList.length))
//      val inFile = "FcdBusProxyMessage." + randomTmp + ".log"
//      val textFile: RDD[String] = sc.textFile("hdfs://192.168.1.51/user/data/GPS/" + inFile)//.cache()
//      val phoneNum: RDD[String] = textFile.map(line => {
//        val splitResult = line.split("\\*")
//        val arrBuffer = ArrayBuffer[String]()
//        if (splitResult.isEmpty) {
//          println("----------------------------------------------- splitResult.isEmpty ")
//        }
//        else {
//          for (i <- 1 until (splitResult.length - 1)) {
//            arrBuffer += splitResult(i).split(",")(1)
//          }
//        }
//        arrBuffer
//      }
//      ).flatMap(line => line)
//
//      val phoneNumSet = phoneNum.collect().toSet
//      val phoneNumArray1 = phoneNumSet.toList
//      val phoneNumArray2 = scala.util.Random.shuffle(phoneNumArray1)//打乱顺序
//
//      var hashMap1 = HashMap[String, String]()
//      var keyValue = ArrayBuffer[String]()
//      for (k <- phoneNumArray1.indices) {
//        hashMap1 += (phoneNumArray1(k) -> phoneNumArray2(k))
//        keyValue += (phoneNumArray1(k)+","+phoneNumArray2(k))
//      }
//
//      val broadcastVar: Broadcast[HashMap[String, String]] = sc.broadcast(hashMap1)
//      textFile.map(line => {
//        //println("hashMap1.size: "+hashMap1.size)
//        var line1 = line
//        for(l <- broadcastVar.value) {
//          if(line1.indexOf(l._1)> 0 ) {
//            line1 = line1.replace(l._1, l._2.substring(0,4)+"标记位"+l._2.substring(4,10))
//          }
//        }
//        //去掉标记
//        val value = newDate+line1.replace("标记位","").substring(10)
//        value
//      }).saveAsTextFile("hdfs://192.168.1.51/user/data/GPS/result/" + newDate)
//
//      println("共用时间（秒）："+ (startTime - System.nanoTime())/ 1e9)
////    }  //end for


    val randomTmp = inFList(r.nextInt(inFList.length))
//    val newArgs = args(1)
    val inFile = "FcdBusProxyMessage." + randomTmp + ".log"
//    val inFile = "FcdBusProxyMessage." + newArgs + ".log"
    val textFile: RDD[String] = sc.textFile("hdfs://192.168.1.51/user/data/GPS/" + inFile).cache()
    val phoneNum: RDD[String] = textFile.map(line => {
      val splitResult = line.split("\\*")
      val arrBuffer = ArrayBuffer[String]()
      if (splitResult.isEmpty) {
        println("----------------------------------------------- splitResult.isEmpty ")
      }
      else {
        for (i <- 1 until (splitResult.length - 1)) {
          arrBuffer += splitResult(i).split(",")(1)
        }
      }
      arrBuffer
    }
    ).flatMap(line => line)

    val phoneNumSet = phoneNum.collect().toSet
    val phoneNumArray1 = phoneNumSet.toList
    val phoneNumArray2 = scala.util.Random.shuffle(phoneNumArray1)//打乱顺序

    var hashMap1 = HashMap[String, String]()
    var keyValue = ArrayBuffer[String]()
    for (i <- phoneNumArray1.indices) {
      hashMap1 += (phoneNumArray1(i) -> phoneNumArray2(i))
      keyValue += (phoneNumArray1(i)+","+phoneNumArray2(i))
    }

    val broadcastVar: Broadcast[HashMap[String, String]] = sc.broadcast(hashMap1)
    textFile.map(line => {
      //println("hashMap1.size: "+hashMap1.size)
     var line1 = line
      for(i <- broadcastVar.value) {
        if(line1.indexOf(i._1)> 0 ) {
          line1 = line1.replace(i._1, i._2.substring(0,4)+"标记位"+i._2.substring(4,10))
        }
      }
      //去掉标记
      val value = newDate+line1.replace("标记位","").substring(10)
      value
      }).saveAsTextFile("hdfs://192.168.1.51/user/data/GPS/result" + newDate)
    //repartition(1).sortBy(identity, true,1).saveAsTextFile("hdfs://192.168.1.51/user/data/GPS/result" + newDate)

    println("共用时间（秒）："+ (startTime - System.nanoTime())/ 1e9)

    } // main func

  def stringToDate(dateString:String): Date= {

    val fmt:DateFormat =new SimpleDateFormat("yyyyMMddHHmmss")
    val date = fmt.parse(dateString)
    date
  }
  def dateToString(date:Date): String= {

    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateString = sdf.format(date)
    dateString
  }

  def getDatesBetweenTwoDate(beginDate: Date, endDate: Date): Array[String] ={
    val fmt: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var lDate = new ArrayBuffer[String]()
    lDate+= fmt.format(beginDate)
    //把开始时间加入集合
    val cal: Calendar = Calendar.getInstance
    //使用给定的 Date 设置此 Calendar 的时间
    cal.setTime(beginDate)
    var bContinue = true
    while (bContinue) {
      //根据日历的规则，为给定的日历字段添加或减去指定的时间量
      cal.add(Calendar.DAY_OF_MONTH, 1)
      // 测试此日期是否在指定日期之后
      if (endDate.after(cal.getTime)) {
        //lDate += cal.getTime.toString
        lDate += fmt.format( cal.getTime)
      } else {
        bContinue = false
      }
    }
    lDate+= fmt.format(endDate)
    lDate.toArray
  }
}


  /**
    * Created by X on 2016/12/16.
    * 1.0 版本正常切分，并按ID合并GPS轨迹并存储到桌面
    */

  import scala.collection.mutable.HashMap
  import scala.collection.mutable.ArrayBuffer
  import org.apache.hadoop.fs.Path
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.{SparkConf, SparkContext}
  object FakeData {

    def main(args: Array[String]) {
      // input date for program: 2016-12-02
      val newDate   = args(0)
      // Start Time
      val t0        = System.nanoTime()
      // Spark conf
      val conf      = new SparkConf().setAppName("FakeData")
      val sc        = new SparkContext(conf)
      // lib of id
      var idArray   = new ArrayBuffer[String]()
      // make data structure like (id,[v1, v2, v3, ...])  FcdBusProxyMessage.2016-12-11.log
      println("begin parse old data ~")
      val sourceRDD = sc.textFile("file:///Users/X/Desktop/temp.log").flatMap( line => {
        val items             = line.split("\\*")
        var retHashMap        = HashMap[String, String]()
        val dateAndServerInfo = items(0)
        for (i <- 1 until  (items.length -1)) {
          val id      = items(i).split(",")(1)
          val value   = dateAndServerInfo + "*" + items(i)
          idArray     +=id
          retHashMap  +=((id, value))
        }
        retHashMap
      }).reduceByKey(_+ "@" +_).sortByKey()
      // out put to file
      sourceRDD.saveAsTextFile("file:///Users/X/Desktop/midResult")
      // select new id in random way
      val r = scala.util.Random
      val newID = idArray(r.nextInt(idArray.length - 1))
      // make new data
      println("begin make new data ~")
      val resultRDD = sourceRDD.flatMap(x=>{
        var retHashMap        = HashMap[String, String]()
        val items = x._2.split("\\@")
        for (i <- 1 until  (items.length - 1)) {
          // tmpResult(0): 2016-12-11 00:00:00.474 - /125.88.128.197:60514:
          // tmpResult(1): 111,18926903037
          val tmpResult     = items(i).split("\\*")
          var newDateAndServer = newDate + tmpResult(0).splitAt(10)._2
          var gpsData       = tmpResult(1).split(",")
          // gpsData(0): 111
          // gpsDate(1): oldID
          var newGPSData = "*" + gpsData(0) + newID
          for (i <- 2 until(gpsData.length - 1)) {  // make new GPS location data
            newGPSData += gpsData(i)
          }
          val newItem = newDateAndServer + newGPSData
          retHashMap += ((newDateAndServer, newGPSData))
        }
        retHashMap
      }).reduceByKey(_+_).sortByKey().saveAsTextFile("file:///Users/X/Desktop/newData")
      // print out Elapsed time
      println("Elapsed time: " + (System.nanoTime() - t0) / 1e9 + " s")
    }
  }

/**
  * Created by will on 16-12-16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.HashMap
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import scala.collection.mutable.ArrayBuffer

object mainScala {

  def main(args: Array[String]) {

    val startTime = System.nanoTime()//用于记录运行时间

    val newDate   = args(0)  //2016-12-02
    val beforeNewDate = args(1) //2016-12-01

    // 读入文件名
    //FcdBusProxyMessage.2016-12-11.log
    //FcdTaxiProxyMessage.2016-12-14.log
    //BUSGPS_01-01-16.txt  BUS
    //FCDGPS_02-06-16.txt  Taxi
    //val inFList    = List("2016-12-07","2016-12-08","2016-12-09","2016-12-11")
    //inFlist for BUSGPS
    val inFList = List("01-01-16","02-01-16","03-01-16","04-01-16", "05-01-16", "06-01-16", "07-01-16", "08-01-16", "09-01-16")
    ////inFlist for TaxiGPS
    //    val inFList = List("01-06-16","02-06-16","03-06-16","04-06-16", "05-06-16", "06-06-16", "07-06-16", "08-06-16", "09-06-16")
    val r     = scala.util.Random

    val conf = new SparkConf().setAppName("daming")//.set("spark.executor.memory","6g")
    val sc = new SparkContext(conf)

    //构造随机读入文件名
    val randomTmp = inFList(r.nextInt(inFList.length))

    //infile for busgps
    val inFile = "BUSGPS_" + randomTmp + ".txt"
    //infile for taxigps
    //    val inFile = "FCDGPS_" + randomTmp + ".txt"
    val textFile: RDD[String] = sc.textFile("hdfs://192.168.1.51/user/data/GPS/" + inFile)
    //val textFile: RDD[String] = sc.textFile("file:////home/will/" + "BUSGPS_01-01-16.txt")

    // id 为车牌信息构成的string
    // Businfo为整个解析后的json map
    val id : Array[String] = textFile.map(line => {
      val BusInfo: Map[String, String] = parseJson(line)
      val vehicleNo: String = BusInfo("vehicleNo")
      val vehicleId: String = BusInfo("vehicleId")
      val vehicleCode: String = BusInfo("vehicleCode")
      val id = vehicleNo + "," + vehicleId + "," + vehicleCode
      id
    }).collect()

    println("输出id: " + id.length)
    val idArray1 = id.toSet.toList

    println("输出idArray1: " + idArray1.length)
    val idArray2 = scala.util.Random.shuffle(idArray1)//打乱顺序[

    var idHashMap = HashMap[String, String]()
    for (i <- idArray1.indices) {
      idHashMap += (idArray1(i) -> idArray2(i))
    }
    println("输出idHashMap: " + idHashMap.size)
    val bcVal: Broadcast[HashMap[String, String]] = sc.broadcast(idHashMap)

    val textFile2: RDD[String] = sc.textFile("hdfs://192.168.1.51/user/data/GPS/" + inFile)
    //val textFile2: RDD[String] = sc.textFile("file:////home/will/" + "BUSGPS_01-01-16.txt")
    // id 为车牌信息构成的string
    // Businfo为整个解析后的json map
    textFile2.map(line => {
      val jsonMap: Map[String, String] = parseJson(line)
      //val jsonMap: Map[String, String] = line._2
      var newJsonMap = jsonMap
      val oldDTmp = randomTmp.split("-")
      val inFileDate = "20" + oldDTmp(2) + "-" + oldDTmp(1) + "-" + oldDTmp(0) //2016-01-02
      //oldTime（每一行读取到的日期） 对比读入文件名中的日期，确定当前行使用当天还是前一天
      //val oldTime = jsonMap.get("time").toString.split(" ")
      val oldTime: Array[String] = jsonMap("time").split(" ")
      for (i <- bcVal.value) {
        // val oldId = jsonMap.get("vehicleNo").toString
        val oldId = jsonMap("vehicleNo")

        val oldIdInHashMap = i._1.split(",")(0) //Str1 -> vehicleNo
        val newIdArr = i._2.split(",") //Str2 -> vehicleNo, vehicleId, vehicleCode
        // oldId为每一行的车牌号，对比之前构造的HashMap中的ID来执行替换操作
        if(oldId.equals(oldIdInHashMap)) {
          // 执行替换操作
          newJsonMap = newJsonMap.updated("vehicleNo", newIdArr(0))
          newJsonMap = newJsonMap.updated("vehicleId", newIdArr(1))
          newJsonMap = newJsonMap.updated("vehicleCode", newIdArr(2))
          if (oldTime(0).equals(inFileDate)) { //当天的日期
          val newTime = newDate + " " + oldTime(1)
            newJsonMap = newJsonMap.updated("time", newTime)
          }
          else { //前一天的日起
          val newTime = beforeNewDate + " " + oldTime(1)
            newJsonMap = newJsonMap.updated("time", newTime)
          }
        }
      }
      // 这种形式转换后 key的顺序会不同于原数据
      // val ret = JSONObject(newJsonMap).toString()
      val ret = busMapToJsonStr(newJsonMap)
      ret
      //newJsonMap
    }).saveAsTextFile("hdfs://192.168.1.51/user/data/GPS/result/BusNew/DaMingData1534/" + newDate)
    println("共用时间（秒）："+ (startTime - System.nanoTime())/ 1e9)
  } // main func

  //Json Map 转换成String 函数
  //主要用于排版输出json string的格式
  def taxiMapToJsonStr(map: Map[String, Any]): String = {
    val id = map("id").toString
    val time = map("time").toString
    val lon = map("lon").toString
    val speed = map("speed").toString
    val dir = map("dir").toString
    val status = map("status").toString
    val alt = map("alt").toString
    val vehicleType = map("vehicleType").toString
    val carry = map("carry").toString
    val vehicleNo = map("vehicleNo").toString
    val lat = map("lat").toString

    val ret = "{" + "\"id\""            + ":" + "\"" + id          + "\"" + "," +
      "\"time\""          + ":" + "\"" + time        + "\"" + "," +
      "\"lon\""           + ":" + "\"" + lon         + "\"" + "," +
      "\"speed\""         + ":" + "\"" + speed       + "\"" + "," +
      "\"dir\""           + ":" + "\"" + dir         + "\"" + "," +
      "\"status\""        + ":" + "\"" + status      + "\"" + "," +
      "\"alt\""           + ":" + "\"" + alt         + "\"" + "," +
      "\"vehicleType\""   + ":" + "\"" + vehicleType + "\"" + "," +
      "\"carry\""         + ":" + "\"" + carry       + "\"" + "," +
      "\"vehicleNo\""     + ":" + "\"" + vehicleNo   + "\"" + "," +
      "\"lat\""           + ":" + "\"" + lat         + "\"" + "}"
    ret
  }

  // 解析json字符串为Map
  def parseJson(json: String): Map[String, String] = {
    //val json = {"lastPosition":"8","time":"2016-01-31 20:16:44","lon":"113.510153","vehicleId":"1183","speed":"20.91","currentStop":"新鸿酒店","vehicleCode":"00014909","is_link_up":"2","routeName":"testRoute","vehicleNo":"粤C14909","lat":"22.228081"}
    //去除首尾的{ }
    var retMap = Map[String, String]()
    val jsonArr = json.substring(1, json.length-1).split(",")
    for (str <- jsonArr) {
      val itemArr = str.split(":", 2)
      val key     = itemArr(0).substring(1, itemArr(0).length-1) //去除首尾" "
      val value   = itemArr(1).substring(1, itemArr(1).length-1) //去除首尾" "
      retMap +=(key -> value)
    }
    retMap
  }
  def busMapToJsonStr(map: Map[String, Any]): String = {
    //val json = """{"lastPosition":"8","time":"2016-01-31 20:16:44","lon":"113.510153","vehicleId":"1183","speed":"20.91","currentStop":"新鸿酒店","vehicleCode":"00014909","is_link_up":"2","routeName":"testRoute","vehicleNo":"粤C14909","lat":"22.228081"}"""

    val lastPosition = map("lastPosition").toString
    val time = map("time").toString
    val lon = map("lon").toString
    val vehicleId = map("vehicleId").toString
    val speed = map("speed").toString
    val currentStop = map("currentStop").toString
    val vehicleCode = map("vehicleCode").toString
    val is_link_up = map("is_link_up").toString
    val routeName = map("routeName").toString
    val vehicleNo = map("vehicleNo").toString
    val lat = map("lat").toString

    val ret = "{" + "\"lastPosition\""  + ":" + "\"" + lastPosition + "\"" + "," +
      "\"time\""          + ":" + "\"" + time         + "\"" + "," +
      "\"lon\""           + ":" + "\"" + lon          + "\"" + "," +
      "\"vehicleId\""     + ":" + "\"" + vehicleId    + "\"" + "," +
      "\"speed\""         + ":" + "\"" + speed        + "\"" + "," +
      "\"currentStop\""   + ":" + "\"" + currentStop  + "\"" + "," +
      "\"vehicleCode\""   + ":" + "\"" + vehicleCode  + "\"" + "," +
      "\"is_link_up\""    + ":" + "\"" + is_link_up   + "\"" + "," +
      "\"routeName\""     + ":" + "\"" + routeName    + "\"" + "," +
      "\"vehicleNo\""     + ":" + "\"" + vehicleNo    + "\"" + "," +
      "\"lat\""           + ":" + "\"" + lat          + "\"" + "}"
    ret
  }
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
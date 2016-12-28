/*
 * Created by will on 16-12-16.
 */

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.collection.immutable.HashMap
import scala.util.parsing.json.{JSON, JSONObject}
import scala.collection.mutable.ArrayBuffer


object mainScala {

  def main(args: Array[String]) {

    val startTime = System.nanoTime()//用于记录运行时间

    val newDate   = args(0)  //2016-12-02
    val beforeNewDate = args(1) //2016-12-01

    //FcdBusProxyMessage.2016-12-11.log
    //FcdTaxiProxyMessage.2016-12-14.log
    //BUSGPS_01-01-16.txt  BUS
    //BUSGPS_03-01-16.txt
    //BUSGPS_04-01-16.txt
    //BUSGPS_05-01-16.txt
    //FCDGPS_02-06-16.txt  Taxi
    //val inFList    = List("2016-12-07","2016-12-08","2016-12-09","2016-12-11")
    //inFlist for BUSGPS
    val inFList = List("01-01-16","02-01-16","03-01-16","04-01-16", "05-01-16", "06-01-16", "07-01-16", "08-01-16", "09-01-16")
    ////inFlist for TaxiGPS
//    val inFList = List("01-06-16","02-06-16","03-06-16","04-06-16", "05-06-16", "06-06-16", "07-06-16", "08-06-16", "09-06-16")
    val r     = scala.util.Random

    val conf = new SparkConf().setAppName("FakeBusGPSData Application")//.set("spark.executor.memory","6g")
    val sc = new SparkContext(conf)

    val randomTmp = inFList(r.nextInt(inFList.length))
    //infile for busgps
    val inFile = "BUSGPS_" + randomTmp + ".txt"
    //infile for taxigps
//    val inFile = "FCDGPS_" + randomTmp + ".txt"

    val arrBuffer = ArrayBuffer[String]()

    val idBCArray: Broadcast[ArrayBuffer[String]] = sc.broadcast(arrBuffer)

    val textFile: RDD[String] = sc.textFile("hdfs://192.168.1.51/user/data/GPS/" + inFile)//.cache()
//    val textFile: RDD[String] = sc.textFile("file:///Users/X/Desktop/" + "BUS.GPS.json")//.cache()

    val idRDD: RDD[String] = textFile.map(line => {
      val BusMap = JSON.parseFull(line)
      val BusInfo: Map[String, Any] = BusMap.get.asInstanceOf[Map[String, Any]]
      val vehicleNo = BusInfo.get("vehicleNo").get.asInstanceOf[String]
      val vehicleId = BusInfo.get("vehicleId").get.asInstanceOf[String]
      val vehicleCode = BusInfo.get("vehicleCode").get.asInstanceOf[String]
      val id = vehicleNo + "," + vehicleId + "," + vehicleCode
      id
    })

    val dataMap: RDD[Map[String, Any]] = textFile.map(line => {
      val BusMap = JSON.parseFull(line)
      val BusInfo: Map[String, Any] = BusMap.get.asInstanceOf[Map[String, Any]]
      //val id = taxiInfo.get("id").get.asInstanceOf[String]
      BusInfo
    }
    )//.flatMap(line => line)

    val idSet = idRDD.collect().toSet
    println("idSet: " + idSet.size)
    val idArray1 = idSet.toList
    val idArray2 = scala.util.Random.shuffle(idArray1)//打乱顺序

    var idHashMap = HashMap[String, String]()
    for (i <- idArray1.indices) {
      idHashMap += (idArray1(i) -> idArray2(i))
    }
    println("idHashMap: " + idHashMap.size)
    val bcVal: Broadcast[HashMap[String, String]] = sc.broadcast(idHashMap)
    val writeRDD = dataMap.map(jsonMap => {
      var newJsonMap = jsonMap
      val oldDTmp = randomTmp.split("-")
      val inFileDate = "20" + oldDTmp(2) + "-" + oldDTmp(1) + "-" + oldDTmp(0) //2016-01-02
      val oldTime = jsonMap.get("time").get.asInstanceOf[String].split(" ") //2016-01-01
//      val newTime = newDate + " " + oldTime(1)
       for (i <- bcVal.value) {
         val oldId = jsonMap.get("vehicleNo").get.asInstanceOf[String]
         val oldIdInHashMap = i._1.split(",")(0) //Str1 -> vehicleNo
         val newIdArr = i._2.split(",") //Str2 -> vehicleNo, vehicleId, vehicleCode
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
    }).saveAsTextFile("hdfs://192.168.1.51/user/data/GPS/result/Bus/" + newDate)
    //.saveAsTextFile("file:///Users/X/Desktop/BUS-GPS-json" + newDate)

    println("共用时间（秒）："+ (startTime - System.nanoTime())/ 1e9)
  } // main func

  def taxiMapToJsonStr(map: Map[String, Any]): String = {

    val id = map.get("id").get.asInstanceOf[String]
    val time = map.get("time").get.asInstanceOf[String]
    val lon = map.get("lon").get.asInstanceOf[String]
    val speed = map.get("speed").get.asInstanceOf[String]
    val dir = map.get("dir").get.asInstanceOf[String]
    val status = map.get("status").get.asInstanceOf[String]
    val alt = map.get("alt").get.asInstanceOf[String]
    val vehicleType = map.get("vehicleType").get.asInstanceOf[String]
    val carry = map.get("carry").get.asInstanceOf[String]
    val vehicleNo = map.get("vehicleNo").get.asInstanceOf[String]
    val lat = map.get("lat").get.asInstanceOf[String]

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

  def busMapToJsonStr(map: Map[String, Any]): String = {
    //val json = """{"lastPosition":"8","time":"2016-01-31 20:16:44","lon":"113.510153","vehicleId":"1183","speed":"20.91","currentStop":"新鸿酒店","vehicleCode":"00014909","is_link_up":"2","routeName":"testRoute","vehicleNo":"粤C14909","lat":"22.228081"}"""
    val lastPosition = map.get("lastPosition").get.asInstanceOf[String]
    val time = map.get("time").get.asInstanceOf[String]
    val lon = map.get("lon").get.asInstanceOf[String]
    val vehicleId = map.get("vehicleId").get.asInstanceOf[String]
    val speed = map.get("speed").get.asInstanceOf[String]
    val currentStop = map.get("currentStop").get.asInstanceOf[String]
    val vehicleCode = map.get("vehicleCode").get.asInstanceOf[String]
    val is_link_up = map.get("is_link_up").get.asInstanceOf[String]
    val routeName = map.get("routeName").get.asInstanceOf[String]
    val vehicleNo = map.get("vehicleNo").get.asInstanceOf[String]
    val lat = map.get("lat").get.asInstanceOf[String]

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


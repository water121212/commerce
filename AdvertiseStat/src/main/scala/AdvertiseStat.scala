import java.util.Date

import commons.conf.ConfigurationManager
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AdvertiseStat {



  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    /**
      * StreamingContext.getActiveOrCreate("checkpointPath",Func)
      * 优先从checkpointpath中通过反序列化取得上下文数据
      * 如果没有相应的checkpoint对应的文件夹，就调用Func去创建新的streamingContext
      * 如果有path，但是代码修改过，直接报错，删除原来的path内容再次启动
      */
    //offset写入checkpoint，并且还保存一份到zookeeper
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5)) //5s
    streamingContext.checkpoint("./spark_streaming")
    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString("kafka.topics")
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "adver",

      /**
        * 老版本：largest smallest none
        * 新版本：latest earilst none
        * latest：在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset
        * 如果没有offset，就从最新的数据开始消费
        * earilist：在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset
        * 如果没有offset，就从最早的数据开始消费
        * none    如果没有offset，就直接报错
        */
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //adRealTimeDateDStream: InputDStream[ConsumerRecord[String, String]]
    val adRealTimeDateDStream = KafkaUtils.createDirectStream(streamingContext,
      //定位策略
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))
    // value: timestamp 	  province 	  city        userid         adid
    // Dstream : RDD[String]  timestamp 	  province 	  city        userid         adid
    val adRealTimeValueDStream = adRealTimeDateDStream.map(item => item.value())
    // 根据黑名单的userId去过滤数据
    val adRealTimeFilteredDStream = filterBlackList(sparkSession, adRealTimeValueDStream)
    /**
      * 需求七：广告点击黑名单实时统计
      * 统计每个用户每天对每个ad的点击总数
      * 将大于ad点击量大于100的用户去重加入黑名单
      */
    generateBlackListOnTime(adRealTimeFilteredDStream)
    /**
      * 需求8:累计计算每个省每个城市在一天的对某一广告的点击量
      */
    val key2ProvinceCityClickCountDStream = generateProvinceCityClickount(adRealTimeFilteredDStream)
    /**
      * 需求9：统计每一天各省Top3热门广告
      */
    getProvinceTop3Advertise(sparkSession, key2ProvinceCityClickCountDStream)
    /**
      * 需求10：统计每个小时每分钟的广告点击量
      */
    getPerHourAndMinuteClickCount(adRealTimeFilteredDStream)
    streamingContext.start()
    streamingContext.awaitTermination()
  }


  def filterBlackList(sparkSession: SparkSession, adRealTimeValueDStream: DStream[String]) = {
    adRealTimeValueDStream.transform {
      recordRDD =>
        val blackListUserInfoArray = AdBlacklistDAO.findAll()
        val blackListUserRDD = sparkSession.sparkContext.makeRDD(blackListUserInfoArray).map {
          item => (item.userid, true)
        }
        val userId2RecordRDD = recordRDD.map {
          item =>
            val userId = item.split(" ")(3).toLong
            (userId, item)
        }
        val userId2FilteredRDD = userId2RecordRDD.leftOuterJoin(blackListUserRDD).filter {
          case (userId, (record, bool)) =>
            if (bool.isDefined && bool.get) {
              false
            } else {
              true
            }
        }.map {
          case (userId, (record, bool)) => (userId, record)
        }
        userId2FilteredRDD
    }
  }

  def generateBlackListOnTime(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    //转化为(dateKey + "_" + userId + "_" + adid,1L)
    val key2NumDStream = adRealTimeFilteredDStream.map {
      case (userId, log) =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val date = new Date(timeStamp)
        val dateKey = DateUtils.formatDateKey(date)
        val adid = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)
    }
    // 一天中，每一个用户对于某一个广告的点击次数
    val key2CountDStream = key2NumDStream.reduceByKey(_ + _)
    //写入到数据库
    key2CountDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val clickCountArray = new ArrayBuffer[AdUserClickCount]()
        for (item <- items) {
          val keySplit = item._1.split("_")
          val dateKey = keySplit(0)
          val userId = keySplit(1).toLong
          val adid = keySplit(2).toLong
          val count = item._2
          clickCountArray += AdUserClickCount(dateKey, userId, adid, count)
        }
        AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }
    //  将大于ad点击量大于100的用户去重加入黑名单
    val userIdBlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val dateKey = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong
        val count = AdUserClickCountDAO.findClickCountByMultiKey(dateKey, userId, adid)
        if (count > 100) {
          true
        } else {
          false
        }
    }.map {
      case (key, count) =>
        val userId = key.split("_")(1).toLong
        userId
    }.transform(rdd => rdd.distinct())
    userIdBlackListDStream.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        var blackListArray = new ArrayBuffer[AdBlacklist]()
        for (item <- items) {
          blackListArray += AdBlacklist(item)
        }
        AdBlacklistDAO.insertBatch(blackListArray.toArray)
      }
    }
  }

  def generateProvinceCityClickount(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    // // timestamp 	  province 	  city        userid         adid
    val key2DStream = adRealTimeFilteredDStream.map {
      case (userId, log) =>
        val logSplit = log.split(" ")
        val date = new Date(logSplit(0).toLong)
        val dateKey = DateUtils.formatDateKey(date)
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)
        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }
    val key2ClickCountDStream = key2DStream.updateStateByKey[Long] { (values: Seq[Long], state: Option[Long]) =>
      var newValue = 0L
      if (state.isDefined) {
        newValue = state.get
      }
      for (value <- values) {
        newValue += value
      }
      Some(newValue)
    }
    key2ClickCountDStream.foreachRDD(rdd =>
      rdd.foreachPartition {
        items =>
          val adStatArray = new mutable.ArrayBuffer[AdStat]()
          for (item <- items) {
            val keySplit = item._1.split("_")
            val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong
            adStatArray += AdStat(date, province, city, adid, item._2)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    )
    key2ClickCountDStream
  }


  def getProvinceTop3Advertise(sparkSession: SparkSession, key2ProvinceCityClickCountDStream: DStream[(String, Long)]) = {
    /**
      * DStream.transform ：针对每一个Rdd 了可以做相应的join等操作
      * DStream.map：针对每一条记录，只能改变记录
      * 区别是粒度不同
      */
    val top3Adevertist = key2ProvinceCityClickCountDStream.transform { rdd =>
      // key = dateKey + "_" + province + "_" + city + "_" + adid
      val key2RDD = rdd.map {
        case (key, count) =>
          val keySplit = key.split("_")
          val dateKey = keySplit(0)
          val province = keySplit(1)
          val adid = keySplit(3)
          val newKey = dateKey + "_" + province + "_" + adid
          (newKey, count)
      }
      val key2CountRDD = key2RDD.reduceByKey(_ + _)
      val key2TableRDD = key2CountRDD.map {
        case (newKey, count) => {
          val keySplit = newKey.split("_")
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
          val province = keySplit(1)
          val adid = keySplit(2).toLong
          (date, province, adid, count)
        }
      }
      import sparkSession.implicits._
      key2TableRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_province_click_count")
      val sql = "select date,province,adid,count from (select date,province,adid,count,row_number() over(partition by province order by count desc ) rank  from tmp_province_click_count) t  where rank<=3"
      sparkSession.sql(sql).rdd
    }

    top3Adevertist.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        val top3Array = new mutable.ArrayBuffer[AdProvinceTop3]()
        for (item <- items) {
          val date = item.getAs[String]("date")
          val province = item.getAs[String]("province")
          val adid = item.getAs[Long]("adid")
          val count = item.getAs[Long]("count")
          top3Array += AdProvinceTop3(date, province, adid, count)
        }
        AdProvinceTop3DAO.updateBatch(top3Array.toArray)
      }
    }
    top3Adevertist
  }

  def getPerHourAndMinuteClickCount(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    val keyDStream = adRealTimeFilteredDStream.map{
      case (userId, log) =>
        val logSplit = log.split(" ")
        val date = new Date(logSplit(0).toLong)
        val timeMinute = DateUtils.formatTimeMinute(date)
        val adid = logSplit(4)
        val key = timeMinute + "_" + adid
        (key, 1L)
    }
    val key2ClickCountDStream = keyDStream.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(60),Minutes(1))
    key2ClickCountDStream.foreachRDD{rdd=>
      rdd.foreachPartition{items=>
        val clickTrendArray = new mutable.ArrayBuffer[AdClickTrend]()
        for(item<-items){
          val keySplit = item._1.split("_")
          // yyyyMMddHHmm
          val timeMinute = keySplit(0)
          val date = timeMinute.substring(0, 8)
          val hour = timeMinute.substring(8, 10)
          val minute = timeMinute.substring(10)
          val adid = keySplit(1).toLong
          val clickCount = item._2
          clickTrendArray += AdClickTrend(date, hour, minute, adid, clickCount)
        }
        AdClickTrendDAO.updateBatch(clickTrendArray.toArray)
      }
    }
  }
}
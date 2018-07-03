import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SessionStat {


  def main(args: Array[String]): Unit = {
    // 首先，读取我们的任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm = JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config((sparkConf)).enableHiveSupport().getOrCreate()
    // 通过sparksql获取动作表里的初始数据转化成RDD
    // UserVisitAction(2018-06-23,80,f14889e03ff94f7fbfda6ab113570a0d,8,2018-06-23 13:43:46,null,-1,-1,81,13,null,null,3)
    val actionRDD = getBasicActionData(sparkSession, taskParm)
    //转化为kv结构
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))
    //根据sessionId聚合数据
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    //持久化一下
    sessionId2GroupRDD.cache()
    //关联用户表转化成完整的信息
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupRDD)

    //注册类累加器
    val sessionStatisticAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatisticAccumulator)

    // 第一：对数据进行过滤
    // 第二：对累加器进行累加
    val sessionId2FilteredRDD = getFilterRDD(sparkSession, taskParm, sessionStatisticAccumulator, sessionId2FullInfoRDD)
    println("********************" + sessionId2FilteredRDD.count())
    sessionId2FilteredRDD.foreach(println(_))
    // 需求一：各范围session占比统计
    getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulator.value)

    // 需求二：随机抽取session
    // sessionId2FullInfoRDD：（sessionId, fullInfo） 一个session只有一条数据
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FullInfoRDD)

    // 需求三：Top10热门品类统计
    // sessionId2ActionRDD: 转化为K-V结构的原始行为数据 （sessionId, UserVisitAciton）
    // sessionId2FilteredRDD: 符合过滤条件的聚合数据  (sessionId, fullInfo)
    // 得到符合过滤条件的action数据
    val sessionId2FilteredActionRDD = sessionId2ActionRDD.join(sessionId2FilteredRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }
    val top10Category = getTop10PopularCategories(sparkSession, taskUUID, sessionId2FilteredActionRDD)

    //统计每一个Top10热门品类的Top10活跃session
    getTop10ActiveSession(sparkSession, taskUUID, top10Category, sessionId2FilteredActionRDD)
  }

  def getBasicActionData(sparkSession: SparkSession, taskParm: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
    import sparkSession.implicits._
    // RDD[UserVisitAction]
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sessionId, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null
        val searchKeywords = new StringBuffer("")
        //搜索
        val clickCategries = new StringBuffer("") //点击
      var userId = -1L
        var stepLength = 0L //步长

        for (action <- iterableAction) {
          //时长  步长
          if (userId == -1L) {
            userId = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime
          if (endTime == null || endTime.before(actionTime))
            endTime = actionTime
          // 完成搜索关键词的追加（去重）
          val searchkKeyWord = action.search_keyword
          if (searchkKeyWord != null && searchKeywords.toString.contains(searchkKeyWord))
            searchKeywords.append(searchkKeyWord + ",")
          // 完成点击品类的追加（去重）
          val clickCategory = action.click_category_id
          if (clickCategory != -1L && !clickCategries.toString.contains(clickCategory))
            clickCategries.append(clickCategory + ",")
          stepLength += 1
        }
        val searchKW = StringUtils.trimComma(searchKeywords.toString)
        val clickCG = StringUtils.trimComma(clickCategries.toString)
        // 获取访问时长(s)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 字段名=字段值|字段名=字段值|
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map { item =>
      (item.user_id, item)
    }
    // userId2AggrInfoRDD -> (userId, aggrInfo)
    // userId2UserInfo -> (userId, UserInfo)
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2UserInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
    }
    sessionId2FullInfoRDD
  }

  def getFilterRDD(sparkSession: SparkSession, taskParm: JSONObject,
                   sessionStatisticAccumulator: SessionStatAccumulator,
                   sessionId2FullInfoRDD: RDD[(String, String)]) = {
    val startAge = ParamUtils.getParam(taskParm, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParm, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParm, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParm, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParm, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParm, Constants.PARAM_KEYWORDS)
    val clickCategories = ParamUtils.getParam(taskParm, Constants.PARAM_CATEGORY_IDS)
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories else "")
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    val sessionId2FilteredRDD = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        }
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false
        if (success) {
          //计算符合条件有多少个session
          sessionStatisticAccumulator.add(Constants.SESSION_COUNT)

          def calculateVisitLength(visitLength: Long) = {
            if (visitLength >= 1 && visitLength <= 3) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visitLength >= 4 && visitLength <= 6) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visitLength >= 7 && visitLength <= 9) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visitLength >= 10 && visitLength <= 30) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visitLength > 30 && visitLength <= 60) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visitLength > 60 && visitLength <= 180) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visitLength > 180 && visitLength <= 600) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visitLength > 600 && visitLength <= 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visitLength > 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }

          def calculateStepLength(stepLength: Long): Unit = {
            if (stepLength >= 1 && stepLength <= 3) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
            } else if (stepLength >= 4 && stepLength <= 6) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
            } else if (stepLength >= 7 && stepLength <= 9) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
            } else if (stepLength >= 10 && stepLength <= 30) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
            } else if (stepLength > 30 && stepLength <= 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
            } else if (stepLength > 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength)
          calculateStepLength(stepLength)

        }
        success
    }
    sessionId2FilteredRDD
  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    //从累加器中取值
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)
    //计算
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    statRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat")
      .mode(SaveMode.Append)
      .save()
  }


  //需求二：随机抽取session
  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FullInfoRDD: RDD[(String, String)]): Unit = {
    //将(sessionId,fullInfo)==》(dateHour,fullInfo)
    val dateHour2FullInfoRDD = sessionId2FullInfoRDD.map {
      case (sessionId, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }
    // countByKey返回一个Map，记录了每个小时的session个数
    val dateHourCountMap = dateHour2FullInfoRDD.countByKey()
    // 定义嵌套Map结构：第一层key为date，第二层key为hour value为每小时的session数量
    val dateHourSessionCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for ((dateHour, count) <- dateHourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      dateHourSessionCountMap.get(date) match {
        case None =>
          dateHourSessionCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourSessionCountMap(date) += (hour -> count)
        case Some(map) =>
          dateHourSessionCountMap(date) += (hour -> count)
      }
    }

    //计算每个小时抽取哪几条---》每小时需要抽取的session数量
    // 每小时需要抽取的session数量 = （每小时的session数量/每天的session数量）* 每天抽取session的数量
    //每天抽取的session数量 =总共需要抽取100个session/ 共有多少天
    val sessionExtractCountPerDay = 100 / dateHourSessionCountMap.size
    val random = new Random()
    // 定义嵌套Map结构存放每天每小时取随机抽样session的下标(date, (hour, List(index1, index2, index3, ...)))
    val dateHourRandomIndexMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()

    def generateRandomIndex(sessionExtractCountPerDay: Int,
                            sessionCountPerDay: Long,
                            hourCountMap: mutable.HashMap[String, Long],
                            hourIndexMap: mutable.HashMap[String, ListBuffer[Int]]) = {
      for ((hour, count) <- hourCountMap) {
        var hourExtractSessionCount = (count / sessionCountPerDay.toDouble * sessionExtractCountPerDay).toInt
        if (hourExtractSessionCount > count) {
          hourExtractSessionCount == count
        }
        hourIndexMap.get(hour) match {
          case None => hourIndexMap(hour) = new mutable.ListBuffer[Int]
            for (i <- 0 until hourExtractSessionCount) {
              var index = random.nextInt(count.toInt)
              while (hourIndexMap(hour).contains(index)) {
                index = random.nextInt(count.toInt)
              }
              hourIndexMap(hour) += index
            }
        }
      }
    }

    for ((date, hourCountMap) <- dateHourSessionCountMap) {
      val sessionCountPerDay = hourCountMap.values.sum //每天的session数量
      dateHourRandomIndexMap.get(date) match {
        case None =>
          dateHourRandomIndexMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()
          generateRandomIndex(sessionExtractCountPerDay, sessionCountPerDay, hourCountMap, dateHourRandomIndexMap(date))
      }
    }

    // 我们知道了每个小时要抽取多少条session，并且生成了对应随机索引的List
    val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()
    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val indexList = dateHourRandomIndexMap.get(date).get(hour)
        val extractSessionArray = new mutable.ArrayBuffer[SessionRandomExtract]()
        var index = 0
        for (fullInfo <- iterableFullInfo) {
          if (indexList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            extractSessionArray += SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
          }
          index += 1
        }
        extractSessionArray
    }
    import sparkSession.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract")
      .mode(SaveMode.Append)
      .save()
  }


  // 需求三：Top10热门品类统计
  def getTop10PopularCategories(sparkSession: SparkSession, taskUUID: String,
                                sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    // 首先得到所有被点击、下单、付款过的品类
    var categoryId2CidRDD = sessionId2FilteredActionRDD.flatMap {
      case (sessionId, action) =>
        val categoryArray = new mutable.ArrayBuffer[(Long, Long)]()
        if (action.click_category_id != -1L) {
          categoryArray += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (cid <- action.order_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (cid <- action.pay_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        }
        categoryArray
    }
    // 所有被点击、下单、付款的品类（不重复）
    // (cid, cid) 不重复
    categoryId2CidRDD = categoryId2CidRDD.distinct()
    // 统计每一个被点击的品类的点击次数
    val clickCount = getCategoryClickCount(sessionId2FilteredActionRDD)
    // 统计每一个被下单的品类的下单次数
    val orderCount = getCategoryOrderCount(sessionId2FilteredActionRDD)
    // 统计每一个被付款的品类的付款次数
    val payCount = getCategoryPayCount(sessionId2FilteredActionRDD)
    // (cid, fullInfo(cid|clickCount|orderCount|payCount))
    val cid2FullInfo = getFullInfoCount(categoryId2CidRDD, clickCount, orderCount, payCount)
    val sortKey2FullInfo = cid2FullInfo.map {
      case (cid, fullInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        val sortKey = new SortedKey(clickCount, orderCount, payCount)
        (sortKey, fullInfo)
    }
    val top10Category = sortKey2FullInfo.sortByKey(false).take(10)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category)
    val top10CategoryWriteRDD = top10CategoryRDD.map {
      case (sortKey, fullInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        Top10Category(taskUUID, categoryId, clickCount, orderCount, payCount)
    }
    top10CategoryWriteRDD.foreach(println(_))
    import sparkSession.implicits._
    top10CategoryWriteRDD.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save()
    top10Category
  }

  def getCategoryClickCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    //过滤掉所有没有发生过点击的aciton
    val sessionId2FilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) =>
        action.click_category_id != -1
    }
    val cid2NumRDD = sessionId2FilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }
    cid2NumRDD.reduceByKey(_ + _)
  }

  def getCategoryOrderCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) => action.order_category_ids != null
    }
    val cid2Num = sessionIdFilterRDD.flatMap {
      case (sessionId, action) => action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid2Num.reduceByKey(_ + _)
  }

  def getCategoryPayCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) => action.pay_category_ids != null
    }
    val cid2Num = sessionIdFilterRDD.flatMap {
      case (sessionId, action) => action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid2Num.reduceByKey(_ + _)
  }

  def getFullInfoCount(categoryId2CidRDD: RDD[(Long, Long)],
                       clickCount: RDD[(Long, Long)],
                       orderCount: RDD[(Long, Long)],
                       payCount: RDD[(Long, Long)]) = {
    val clickCountRDD = categoryId2CidRDD.leftOuterJoin(clickCount).map {
      case (categoryId, (cid, cCount)) =>
        val count = if (cCount.isDefined) cCount.get else 0L
        val aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + count
        (categoryId, aggrInfo)
    }
    val orderCountRDD = clickCountRDD.leftOuterJoin(orderCount).map {
      case (categoryId, (aggrInfo, oCount)) =>
        val count = if (oCount.isDefined) oCount.get else 0L
        val orderInfo = aggrInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
        (categoryId, orderInfo)
    }

    val payCountRDD = orderCountRDD.leftOuterJoin(payCount).map {
      case (categoryId, (orderInfo, pCount)) =>
        val count = if (pCount.isDefined) pCount.get else 0L
        val payInfo = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + count
        (categoryId, payInfo)
    }
    payCountRDD
  }

  def getTop10ActiveSession(sparkSession: SparkSession, taskUUID: String, top10Category: Array[(SortedKey, String)],
                            sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): Unit = {
    //    top10Category:  top10品类  RDD[(sortKey, fullInfo)]
    //    sessionId2FilteredActionRDD:   符合筛选条件的[sessionId,UserVisitAction]
    /**
      * 第一步：过滤已经只剩下点击过Top10品类的数据(sessionId, action)
      *   1.top10品类  RDD[(sortKey, fullInfo)]转换为(cid, cid)
      *   2.将过滤后的原始数据转化为 (cid, action),如果没有点击过cid为-1L
      *   3.top10品类通过内连接原始数据 过滤为只点击过op10品类的数据
      *   val sessionId2FilteredRDD = cid2ActionRDD.join(top10CategoryRDD).
      */
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category).map {
      case (sortKey, fullInfo) =>
        val cid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        (cid, cid)
    }
    val cid2ActionRDD = sessionId2FilteredActionRDD.map {
      case (sessionId, action) =>
        val cid = action.click_category_id
        (cid, action)
    }
    //已经只剩下点击过Top10品类的session中的action数据
    val sessionId2FilteredRDD = cid2ActionRDD.join(top10CategoryRDD).map {
      case (cid, (action, categoryId)) =>
        val sessionId = action.session_id
        (sessionId, action)
    }

    /**
      *  第二步：统计每个session对Top10热门品类的点击次数
      *  1.对过滤后的(sessionId, action)进行聚合  ------(sessionId, iterableAction)
      *  2.构建categoryCountMap 存储每个session中对top10品类的点数数量 (cid,count)
      *    通过yield (cid, sessionId + "=" + count) 将每个session中的数据返回
      *  3.对cid2SessionCountRDD进行聚合----(cid,iterableSessionCount)，
      *    对iterableSessionCount进行排序，获取每个cid对应session中最活跃的前10个
      *    并封装为case Class 返回 (Top10Session)
      *  4.将所有返回的数据写入数据库
      */
    val sessionId2GroupRDD = sessionId2FilteredRDD.groupByKey()
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction) {
          val categoryId = action.click_category_id
          if (!categoryCountMap.contains(categoryId))
            categoryCountMap += (categoryId -> 0)
          categoryCountMap.update(categoryId, categoryCountMap(categoryId) + 1)
        }
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }
    val cid2GroupSessionCountRDD =cid2SessionCountRDD.groupByKey()
    val top10SessionRDD = cid2GroupSessionCountRDD.flatMap{
      case(cid,iterableSessionCount)=>
       val sortedSession  =iterableSessionCount.toList.sortWith{
         (item1,item2)=>
           item1.split("=")(1).toLong>item2.split("=")(1).toLong
       }
        val top10Session = sortedSession.take(10)
        val top10SessionCaseClass = top10Session.map{
          // item: String session=count
          item =>
            val sessionid = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionid, count)
        }

        top10SessionCaseClass
    }
    import  sparkSession.implicits._
    top10SessionRDD.toDF.write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .mode(SaveMode.Append).save()
  }
}

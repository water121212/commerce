import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageStat {



  def main(args: Array[String]): Unit = {
    // 获取任务限制参数
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam =  JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString
    val sparkConf  = new SparkConf().setAppName("PageStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    /**
      * 需求五：统计页面跳转转化率
      */
    /**
      * 获取配置文件中的 pageFlow: String  1,2,3,4,5,6,7
      * 通过拉链操作将数据转化为Array[String]  (1_2,2_3,....)
      */
    val pageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val pageFlowArray  = pageFlow.split(",")
    val targetPageSplit:Array[String] = pageFlowArray.slice(0,pageFlowArray.length-1).zip(pageFlowArray.tail).map{
       case(page1,page2)=>page1+"_"+page2
    }
    /**
      * 1.读取数据 ------转换为[sessionId.action]
      * 2.将[sessionId.action]进行聚合------[sessionId.iterableAction]
      * 3.统计每个页面跳转的访问数量(page,count)
      *    3.1将session中action 根据action中的时间将进行排序
      *    3.2将每一个session中的排序后的iterableAction根据过滤page条件进行过滤，并转化为[page,1]  以便进行页面跳转的计数(1_2,1)
      *    3.3对所有的[page,1] 进行统计
      *    pageCountMap = pageFilteredSplit.countByKey()//返回map 方便数据读取 所以不用 reduceBy(返回RDD)
      */
    val sessionId2ActionRDD = getSessionAction(sparkSession, taskParam)
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    val pageFilteredSplit = sessionId2GroupRDD.flatMap{
      case(sessionId,iterableAction)=>
        val storedAction = iterableAction.toList.sortWith((action1,action2) =>
           DateUtils.parseTime(action1.action_time).getTime<
           DateUtils.parseTime(action2.action_time).getTime
        )
        val pageFlow =  storedAction.map(item=>item.page_id)
        val pageSplit = pageFlow.slice(0,pageFlow.length-1).zip(pageFlow.tail).map{
           case(page1,page2)=>page1 + "_" + page2
        }
        val pageFilteredSplit =pageSplit.filter(item=>targetPageSplit.contains(item)).map{
          item=>(item,1L)
        }
        pageFilteredSplit
    }
    val pageCountMap = pageFilteredSplit.countByKey()


    /**
      *计算页面跳转率 ：当前页面的访问数量/上一个页面的访问数量
      * 1.计算访问首页时的数量
      * 2.循环targetPageSplit数组，计算每一个页面的跳转率，并将结果保存在Map中
      * 3.将pageConvertMap转化为字符串，并封装为case类
      * 4.写入数据库
      */
    val startPage = pageFlowArray(0).toLong
    val startPageCount = sessionId2ActionRDD.filter{
      case(sessionId,action)=>
        action.page_id==startPage
    }.count()
    // pageConvertMap：用来保存每一个切片对应的转化率
    var lastCount = startPageCount.toDouble
    val pageConvertMap = new mutable.HashMap[String, Double]()
    for (pageSplit<-targetPageSplit){
       var currentCount = pageCountMap(pageSplit).toDouble
      var rate = currentCount/lastCount
      pageConvertMap += (pageSplit->rate)
      lastCount = currentCount
    }
    var rateStr = pageConvertMap.map{
      case (pageSplit,rate)=>pageSplit+"="+rate
    }.mkString("|")
    val pageSplit = PageSplitConvertRate(taskUUID,rateStr)
    val pageSplitNewRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))
    import sparkSession.implicits._
    pageSplitNewRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
    sessionId2ActionRDD.foreach((item)=> println(item))
  }
  def getSessionAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date>='"+startDate+"' and date<='"+endDate+"'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item=>(item.session_id,item))
  }

}

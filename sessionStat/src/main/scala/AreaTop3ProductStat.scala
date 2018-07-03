import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object AreaTop3ProductStat {


  def main(args: Array[String]): Unit = {
    val json = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(json)
    val taskUUID = UUID.randomUUID().toString
    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    /**
      * 需求六：各区域Top3商品统计
      */
    //(从原始数据中过滤点击过产品的数据 并转化为====》city_id,[city_id,product_id])(5,[5,15])
    val cityId2ProductInfoRDD = getCityAndProductInfo(sparkSession, taskParam)
    // 根据静态城市区域信息 转化为RDD ===》(city_id,(city_id,南京,华东))
    val cityId2AreaInfoRDD = getCityAndAreaInfo(sparkSession)
    //将 2个RDDjoin起来获得(cityId, cityName, area, product_id)==》sql临时表
    getAreaProductBasicInfo(sparkSession, cityId2ProductInfoRDD, cityId2AreaInfoRDD)
    //自定义拼接函数city_id:city_name
    sparkSession.udf.register("concat_long_string",(v1:Long,v2:String,split:String)=>{
      v1+split+v2
    })
    // 自定义去重函数
    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistionct)
    //通过group by area,product_id统计每个区域每个商品的点击次数和去重后的城市信息
    getAreaProductClickCount(sparkSession)
    sparkSession.udf.register("get_json_value", (str:String, field:String) => {
      val jsonObject = JSONObject.fromObject(str)
      jsonObject.getString(field)
    })
    //统计每个区域每个商品的点击次数和去重后的城市信息与商品信息表关联拼接商品信息
    getProductInfo(sparkSession)
//    sparkSession.sql("select * from tmp_area_product_click_count").show()
//    通过窗口函数row_number() over(partition by area order by click_count desc) 获取各区域Top3商品
    getAreaTop3PopularProduct(sparkSession, taskUUID)
  }

  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select city_id,click_product_id from user_visit_action where date>='" + startDate +
      "' and date<='" + endDate + "' and click_product_id != -1 and click_product_id is not null"
    sparkSession.sql(sql).rdd.map {
      row => (row.getAs[Long]("city_id"), row)
    }
  }

  def getCityAndAreaInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))
    import sparkSession.implicits._
    val cityAreaInfoRDD = sparkSession.sparkContext.makeRDD(cityAreaInfoArray).toDF("city_id", "city_name", "area").rdd.map {
      row => (row.getLong(0), row)
    }
    cityAreaInfoRDD
  }

  def getAreaProductBasicInfo(sparkSession: SparkSession,
                              cityId2ProductInfoRDD: RDD[(Long, Row)],
                              cityId2AreaInfoRDD: RDD[(Long, Row)]) = {
    val areaProductBasicRDD = cityId2ProductInfoRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (productInfo, areaInfo)) =>
        val cityName = areaInfo.getAs[String]("city_name")
        val area = areaInfo.getAs[String]("area")
        val product_id = productInfo.getAs[Long]("click_product_id")
        (cityId, cityName, area, product_id)
    }
    import sparkSession.implicits._
    areaProductBasicRDD.toDF("city_id", "city_name", "area", "product_id")
      .createOrReplaceTempView("tmp_area_product_basic_info")

  }

  //通过group by area,product_id统计每个区域每个商品的点击次数和去重后的城市信息
  def getAreaProductClickCount(sparkSession: SparkSession): Unit = {
    val sql = "select area ,product_id,count(*) click_count,"+
    "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos "+
    " from tmp_area_product_basic_info group by area,product_id"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_click_count")
  }

  def getProductInfo(sparkSession: SparkSession) = {
    // 进行tmp_area_product_click_count与product_info表格的联立
    // if(判断条件, 为ture执行, 为false执行)
    val sql = "select tapcc.area, tapcc.city_infos, tapcc.product_id, tapcc.click_count, pi.product_name," +
      "if(get_json_value(pi.extend_info, 'product_status')='0', 'Self', 'Third Party') product_status " +
      "from tmp_area_product_click_count tapcc join product_info pi on tapcc.product_id = pi.product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_info")
  }

  def getAreaTop3PopularProduct(sparkSession: SparkSession, taskUUID: String): Unit = {
    val sql = "select area, " +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A Level' " +
      "WHEN area='华南' OR area='华中' THEN 'B Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level, product_id, city_infos, click_count, product_name, product_status from(" +
      " select area, product_id, city_infos, click_count, product_name, product_status, " +
      " row_number() over(partition by area order by click_count desc) rank from tmp_area_product_info) t" +
      " where rank<=3"
    sparkSession.sql(sql).show()
    val areaTop3ProductRDD = sparkSession.sql(sql)
      .rdd
    val areaTop3RDD = areaTop3ProductRDD.map{
      row =>
        AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
          row.getAs[Long]("product_id"), row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
    }
    import sparkSession.implicits._
    areaTop3RDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }
}

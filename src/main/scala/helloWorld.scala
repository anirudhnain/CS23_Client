import scalaj.http.Http

import scala.io.Source
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import jp.co.bizreach.s3scala.S3
import awscala.s3._
import java.net.URL
import org.apache.commons.io.IOUtils
import com.amazonaws.services.s3.model.{CSVInput, CSVOutput, ExpressionType, InputSerialization, OutputSerialization, SelectObjectContentRequest, SelectObjectContentResult}
import awscala.Region

object helloWorld {

  def main(args: Array[String]): Unit = {

//    var sparkConfig = new SparkConf().setMaster("local[4]").setAppName("downloadCSV")
//    var sc  = new SparkContext(sparkConfig)
//
//    callURL(sc, sparkConfig, "https://speedfs.s3.us-east-2.amazonaws.com/parking-citations.csv")
//    callURL(sc, sparkConfig, "http://172.31.41.137:8100/data/parking-citations.csv")
    makeS3SelectQuery("parking-citations-500k.csv")
  }

  def makeS3SelectQuery(file: String): Unit = {
    println("Making Query")
    implicit val region = Region.US_EAST_2
    implicit val s3 = S3(accessKeyId = "AKIAJKMERBIBTXB2PVEA", secretAccessKey = "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")
    val request =  new SelectObjectContentRequest()
      .withBucketName("speedfs")
      .withKey(file)
      .withExpression(buildQuery("Color","GY"))
      .withExpressionType(ExpressionType.SQL)

    val inputSerialization = new InputSerialization()
    inputSerialization.setCsv(new CSVInput().withFileHeaderInfo("USE"))
    request.setInputSerialization(inputSerialization)

    val outputSerialization = new OutputSerialization()
    outputSerialization.setCsv(new CSVOutput())
    request.setOutputSerialization(outputSerialization)

    val t0 = System.currentTimeMillis()

    try {
      val result = s3.selectObjectContent(request).getPayload.getRecordsInputStream
      try {
        println("Size: " + IOUtils.readLines(result, "UTF-8").size)
      } finally {
        result.close()
      }
    }
    println("Spark Time taken: " + (System.currentTimeMillis()-t0))
  }

  def buildQuery(columnName: String, columnNameValue: String): String = {
    val baseQuery = new StringBuilder("select * from S3Object s where 1=1 ")
    if (columnName != null) {
      baseQuery.append(s"and s.${columnName} = '${columnNameValue}'")
    }
    return baseQuery.mkString("")
  }

  def callURL(sc: SparkContext, sparkConfig:SparkConf, url: String): Unit = {

      var fileSize = 1024*1024*1024
      var blockSize = 1024*1024*8

      val spark = SparkSession.builder
        .config(sparkConfig)
        .getOrCreate;

      val requestProperties = Map("Range"->s"bytes=0-${fileSize-1}")

      val connection = new URL(url).openConnection

      requestProperties.foreach({
        case (name, value) => connection.setRequestProperty(name,value)
      })
      val t1 = System.currentTimeMillis()

      Source.fromInputStream(connection.getInputStream).getLines().mkString("\n")

      println("URL: " + url  + " direct call Time taken: " + (System.currentTimeMillis()-t1))

      import spark.implicits._

      var requestRdd = sc.parallelize(makeUrls(fileSize, blockSize)).toDS()

      val t0 = System.currentTimeMillis()
      requestRdd.foreach(download(_, url))

      println("Spark Time taken: " + (System.currentTimeMillis()-t0))
  }

  def makeUrls(fileSize: Int, blockSize: Int): List[Seq[(String, String)]]={

    var headerList = ListBuffer[Seq[(String, String)]]()

    for (i <- 0 until fileSize-1 by blockSize) {
      var rangeRequest = s"bytes=$i-${i + blockSize-1}"
      val requestHeader = Seq("Range" -> rangeRequest)
      headerList+=requestHeader
    }
    return headerList.toList
  }

  def download(requestHeader: Seq[(String, String)], url: String): Unit = {

    val connection = new URL(url).openConnection

    requestHeader.foreach({
      case (name, value) => connection.setRequestProperty(name,value)
    })
    Source.fromInputStream(connection.getInputStream).getLines().mkString("\n")
  }
}
import scalaj.http.Http

import scala.io.Source
import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import jp.co.bizreach.s3scala.S3
import awscala.s3._
import java.net.URL
import sys.process._
import org.apache.commons.io.IOUtils
import com.amazonaws.services.s3.model.{CSVInput, CSVOutput, ExpressionType, InputSerialization, OutputSerialization, SelectObjectContentRequest, SelectObjectContentResult}
import awscala.Region

object scalaClient {

  def main(args: Array[String]): Unit = {

//    var sparkConfig = new SparkConf().setMaster("local[4]").setAppName("downloadCSV")
//    var sc  = new SparkContext(sparkConfig)
//
//    callURL(sc, sparkConfig, "https://speedfs.s3.us-east-2.amazonaws.com/parking-citations.csv")
//    callURL(sc, sparkConfig, "http://ec2-18-220-77-181.us-east-2.compute.amazonaws.com:8100/data/parking-citations.csv")
//    makeS3SelectQuery("copy-parking-citations-500k.csv")

    var serverAPI = args(0)

    if (serverAPI.equals("S3"))
      makeS3SelectQuery(args(1), args(2), args(3))
    else if ( serverAPI.equals("Frontier"))
      makeFrontierSelectQuery(args(1), args(2), args(3))
    else
      println("Incorrect command line arguments passed. Please pass which system to user, provided by fileName, columnName and columnValue")

//      val upload0 = System.currentTimeMillis()

//      uploadTOS3()
//
//    println("upload Time: " + (System.currentTimeMillis()-upload0))
  }

  def makeFrontierSelectQuery(file: String, columnName: String, columnValue: String): Unit = {

    println("Frontier called file: " + file)
//    query/

    var url = "http://ec2-18-220-77-181.us-east-2.compute.amazonaws.com:8100/data/parking-citations-500k.csv"

    var connection = new URL(url).openConnection

    var requestHeader = Seq("Range"->"bytes=0-1024")

    requestHeader.foreach({
      case (name, value) => connection.setRequestProperty(name,value)
    })

    var fileHeader: String = Source.fromInputStream(connection.getInputStream).getLines().toList(0)

    val columnIndex = fileHeader.split(",").indexOf(columnName)

    url = s"http://ec2-18-220-77-181.us-east-2.compute.amazonaws.com:8100/data/parking-citations-500k.csv/query/${file}?col=${columnIndex}&condition=${columnValue}"

    connection = new URL(url).openConnection

    println(Source.fromInputStream(connection.getInputStream).getLines().mkString("\n"))
    //    -- arguments col[0 sindex]
//    -- condition :: equal tp
  }

//  def getColumns(): Unit = {
//    val connection = new URL(url).openConnection
//
//    requestHeader.foreach({
//      case (name, value) => connection.setRequestProperty("name",value)
//    })
//    Source.fromInputStream(connection.getInputStream).getLines().mkString("\n")
//  }

  def makeS3SelectQuery(file: String, columnName: String, columnValue: String): Unit = {
    println("Making Query")
    implicit val region = Region.US_EAST_2
    implicit val s3 = S3(accessKeyId = "AKIAJKMERBIBTXB2PVEA", secretAccessKey = "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")
    val request =  new SelectObjectContentRequest()
      .withBucketName("speedfs")
      .withKey(file)
      .withExpression(buildQuery(columnName,columnValue))
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
    println("S3 Select Time taken: " + (System.currentTimeMillis()-t0))
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

//      val requestProperties = Map("Range"->s"bytes=0-${fileSize-1}")
//
//      val connection = new URL(url).openConnection
//
//      requestProperties.foreach({
//        case (name, value) => connection.setRequestProperty(name,value)
//      })
//      val t1 = System.currentTimeMillis()
//
//      Source.fromInputStream(connection.getInputStream).getLines().mkString("\n")
//
//      println("URL: " + url  + " direct call Time taken: " + (System.currentTimeMillis()-t1))

      import spark.implicits._

      val compute0 = System.currentTimeMillis()
      var urls = makeUrls(fileSize, blockSize)
      println("Compute Time : " + (System.currentTimeMillis()-compute0))

      val ephermeralData0 = System.currentTimeMillis()
      var requestRdd = sc.parallelize(urls).toDS()

//      requestRdd.foreach(uploadDS)

      println("Ephemeral Data 0 Time : " + (System.currentTimeMillis()-ephermeralData0))

      val t0 = System.currentTimeMillis()
      requestRdd.foreach(download(_, url))

      println("Spark Time taken: " + (System.currentTimeMillis()-t0))
  }

  def uploadTOS3(): Unit ={
      println("Upload to S3")
      implicit val region = Region.US_EAST_2
      implicit val s3 = S3(accessKeyId = "AKIAJKMERBIBTXB2PVEA", secretAccessKey = "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")

      var fileContent  = new File("/home/aniru/Downloads/parking-citations.csv")

      s3.putObject("speedfs", "check-parking-500k", fileContent)
  }

  def uploadDS(requestHeader: Seq[(String, String)]): Unit = {
    var response = s"curl -F 'data=${requestHeader.mkString("\n")}' http://ec2-18-220-77-181.us-east-2.compute.amazonaws.com:8100/put/samplefile -X put" !!
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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
//import spark.implicits._
import scala.collection.mutable.ListBuffer
import java.io.File

object FileParser {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master(master="local[2]")
      .getOrCreate()
  def main(args: Array[String]): Unit = {
    val inpDir = "../kp_data"
    val outDir = "./src/main/parquet"

    val files = new File(inpDir).listFiles.filter(_.isFile).toList

    val sessionData = ListBuffer[(String, String, String)]()
    val docOpenData = ListBuffer[(String, String, String, String)]()
    val qsData = ListBuffer[(String, String, String, String)]()
    val qsResultData = ListBuffer[(String, String, String)]()
    val csData = ListBuffer[(String, String, String, String, String)]()
    val csResultData = ListBuffer[(String, String, String)]()

    val batchSize = 50
    var fileCount = 0

    files.foreach { file =>
      val fileName = file.getName
      val lines = scala.io.Source.fromFile(file, "windows-1251").getLines().toList

      var sessionStart = ""
      var sessionEnd = ""
      var docOpenTime = ""
      var docOpenId = ""
      var docOpenDoc = ""
      var qsTime = ""
      var qsText = ""
      var qsId = ""
      var csTime = ""
      var csParamId = ""
      var csParamName = ""
      var csId = ""

      var isQS = false
      var isCardStart = false
      var isCardEnd = false

      lines.foreach { line =>
        if (line.startsWith("SESSION_START")) {
          sessionStart = line.split(" ")(1)
        } else if (line.startsWith("SESSION_END")) {
          sessionEnd = line.split(" ")(1)
        } else if (line.startsWith("DOC_OPEN")) {
          val parts = line.split(" ")
          docOpenTime = parts(1)
          docOpenId = parts(2)
          docOpenDoc = parts(3)
          docOpenData += ((fileName, docOpenTime, docOpenId, docOpenDoc))
        } else if (isQS && line.matches("\\d+ .+")) {
          val parts = line.split(" ")
          qsId = parts(0)
          parts.tail.foreach { doc =>
            qsResultData += ((fileName, qsId, doc))
          }
          qsData += ((fileName, qsId, qsTime, qsText))
          isQS = false
        } else if (line.startsWith("QS")) {
          val parts = line.split(" ")
          qsTime = parts(1)
          qsText = line.substring(line.indexOf("{") + 1, line.indexOf("}"))
          isQS = true
        } else if (isCardEnd && line.matches("\\d+ .+")) {
          val parts = line.split(" ")
          csId = parts(0)
          parts.tail.foreach { doc =>
            csResultData += ((fileName, csId, doc))
          }
          csData += ((fileName, csId, csTime, csParamId, csParamName))
          isCardEnd = false
        } else if (line.startsWith("CARD_SEARCH_END")) {
          isCardStart = false
          isCardEnd = true
        } else if (isCardStart && line.startsWith("$")) {
          val parts = line.split(" ", 2)
          csParamId = parts(0)
          csParamName = parts(1)
        } else if (line.startsWith("CARD_SEARCH_START")) {
          val parts = line.split(" ")
          csTime = parts(1)
          isCardStart = true
        }
      }

      sessionData += ((fileName, sessionStart, sessionEnd))

      fileCount += 1

      if (fileCount % batchSize == 0) {
        println(fileCount)
        saveToParquet(spark,
          sessionData.toList,
          docOpenData.toList,
          qsData.toList,
          qsResultData.toList,
          csData.toList,
          csResultData.toList,
          outDir)
        sessionData.clear()
        docOpenData.clear()
        qsData.clear()
        qsResultData.clear()
        csData.clear()
        csResultData.clear()
      }
    }

    if (sessionData.nonEmpty) {
      saveToParquet(spark,
        sessionData.toList,
        docOpenData.toList,
        qsData.toList,
        qsResultData.toList,
        csData.toList,
        csResultData.toList,
        outDir)
    }


    spark.stop
  }
  def saveToParquet(spark: SparkSession,
                    sessionData: List[(String, String, String)],
                    docOpenData: List[(String, String, String, String)],
                    qsData: List[(String, String, String, String)],
                    qsResultData: List[(String, String, String)],
                    csData: List[(String, String, String, String, String)],
                    csResultData: List[(String, String, String)],
                    outputDir: String): Unit = {
    import spark.implicits._

    val sessionDF = sessionData.toDF("fileName", "sessionStart", "sessionEnd")
    val docOpenDF = docOpenData.toDF("fileName", "docOpenTime", "docOpenId", "docOpenDoc")
    val qsDF = qsData.toDF("fileName", "qsId", "qsTime", "qsText")
    val qsResultDF = qsResultData.toDF("fileName", "qsId", "document")
    val csDF = csData.toDF("fileName", "csId", "csTime", "csParamId", "csParamName")
    val csResultDF = csResultData.toDF("fileName", "csId", "document")

    sessionDF.write.mode("append").parquet(s"$outputDir/session")
    docOpenDF.write.mode("append").parquet(s"$outputDir/doc_open")
    qsDF.write.mode("append").parquet(s"$outputDir/qs")
    qsResultDF.write.mode("append").parquet(s"$outputDir/qs_result")
    csDF.write.mode("append").parquet(s"$outputDir/cs")
    csResultDF.write.mode("append").parquet(s"$outputDir/cs_result")
  }
}

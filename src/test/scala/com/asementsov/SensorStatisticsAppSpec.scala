package com.asementsov

import java.io._

import com.asementsov.Row.headerString
import com.asementsov.SensorAggStat._
import com.asementsov.SensorStatisticsApp.Context
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class SensorStatisticsAppSpec extends FlatSpec with Matchers with LazyLogging {

  "SensorStatisticsApp.aggregateFileStat" should "process leader-1.csv" in {
    val inputFile = "input/leader-1.csv"
    val expected = Map("s1" -> singleMeasure(10).copy(nanCount = 1), "s2" -> singleMeasure(88))
    val inputStream: InputStream = getResourceInputStream(inputFile)

    val fileStat: Map[String, SensorAggStat] = SensorStatisticsApp.getStatByFile(Context(inputFile), inputStream)

    assert(fileStat == expected)
  }

  it should "process leader-2.csv" in {
    val inputFile = "input/leader-2.csv"
    val expected = Map("s1" -> singleMeasure(98), "s2" -> SensorAggStat(78, 80, 158, 2, 0), "s3" -> nanStat)
    val inputStream: InputStream = getResourceInputStream("input/leader-2.csv")

    val fileStat: Map[String, SensorAggStat] = SensorStatisticsApp.getStatByFile(Context(inputFile), inputStream)

    assert(fileStat == expected)
  }

  it should "process empty file" in {
    val inputFile = new File(getAbsoluteResourcePath(""), "nodata_leader.csv")
    generateTestFile(inputFile, headerString)

    val fileStat: Map[String, SensorAggStat] = SensorStatisticsApp.getStatByFile(Context(inputFile), new FileInputStream(inputFile))

    assert(fileStat.isEmpty)
  }

  //TODO write a test according to requirements
  // case: out of bound measurement
  // case: not a digital measurement
  // case: an empty measurement/sensor-id
  it should "process file with incorrect data"

  //TODO write a test according to requirements
  it should "process file without header"

  it should "process file that doesn't fit into the memory" in {
    val sensorsCount = 1000
    val inputFile = new File(getAbsoluteResourcePath(""), "huge-leader-0.csv")
    val sampleRow: String = (0 until sensorsCount).map(i => s"s$i,${Random.nextInt(100)}").mkString("\n")
    logger.info("Sample row length: " + sampleRow.length)

    //generate 700Mb file
    val inputFileSizeBytes: Int = 700 * 1000 * 1000
    val times: Int = inputFileSizeBytes / sampleRow.length
    logger.info(inputFileSizeBytes + " bytes file generation")

    generateTestFile(inputFile, headerString, sampleRow, times)
    val inputStream = new FileInputStream(inputFile)

    val fileStat: Map[String, SensorAggStat] = SensorStatisticsApp.getStatByFile(Context(inputFile), inputStream)
    inputFile.delete()

    assert(fileStat("s0").measureCount == times)
    assert(fileStat.size == sensorsCount)
  }

  "SensorStatisticsApp" should "pass the happy path" in {
    val inputDir: String = getAbsoluteResourcePath("input")
    val writer: StringWriter = new StringWriter()
    val expectedResult: String =
      """Num of processed files: 2
        |Num of processed measurements: 7
        |Num of failed measurements: 2
        |
        |Sensors with highest avg humidity:
        |
        |sensor-id,min,avg,max
        |s2,78,82,88
        |s1,10,54,98
        |s3,NaN,NaN,NaN
        |""".stripMargin

    SensorStatisticsApp.startProcess(SensorStatisticsCli(inputDir), new PrintWriter(writer))

    assert(writer.toString == expectedResult)
  }
}
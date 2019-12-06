package com.asementsov

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import com.asementsov.SensorAggStat.nan
import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray
import scala.concurrent.forkjoin.ForkJoinPool

object SensorStatisticsApp extends LazyLogging {

  //contains aggregate statistic by a sensor
  type Cache = ConcurrentHashMap[String, SensorAggStat]

  val batchSize = 10000
  val concurrencyLevel = 4

  //represents application context, contains metadata that can be useful for nested processing
  case class Context(inputFile: File)

  object Context {
    def apply(inputFilePath: String): Context = new Context(new File(inputFilePath).getCanonicalFile)
  }

  def main(args: Array[String]): Unit = {
    val cli: SensorStatisticsCli = SensorStatisticsCli.parseArgs(args)
    val outputStream: PrintStream = System.out

    startProcess(cli, new PrintWriter(outputStream, true))
  }

  def startProcess(cli: SensorStatisticsCli, printWriter: PrintWriter): Unit = {
    val cache: Cache = new ConcurrentHashMap()

    logger.info("Directory to list input files: " + cli.inputDir)
    val inputFiles: ParArray[File] = cli.inputDir.listFiles().par
    logger.info("Count of files to process: " + inputFiles.length)

    val forkJoinPool = new ForkJoinPool(concurrencyLevel)
    inputFiles.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

    inputFiles
      .foreach {
        file =>
          logger.info("Start processing file " + file.getAbsolutePath)
          val inputStream = new FileInputStream(file)

          val fileStats: Map[String, SensorAggStat] = getStatByFile(Context(file), inputStream)

          logger.info(s"File ${file.getName} has ${fileStats.size} unique sensors")
          fileStats.foreach {
            case (key, stat) =>
              cache.merge(key, stat, toJavaBiFunction(SensorAggStat.merge))
          }
      }
    forkJoinPool.shutdown()

    val message: String = generateOutput(inputFiles.length, cache.asScala)
    printWriter.println(message)
  }

  def getStatByFile(context: Context, inputStream: InputStream): Map[String, SensorAggStat] = {
    val settings = new CsvParserSettings
    settings.getFormat.setLineSeparator("\n")
    settings.setHeaderExtractionEnabled(true)

    val parser = new CsvParser(settings)

    val recordIterator: Iterator[Record] = parser
      .iterateRecords(inputStream)
      .iterator()

    val parsedRowsIterator: Iterator[Row] = recordIterator
      .zipWithIndex
      .flatMap {
        case (record, idx) => Row(context.inputFile.getName, record, idx)
      }

    val batchStatIterator: Iterator[Map[String, SensorAggStat]] = parsedRowsIterator
      .grouped(batchSize)
      .map {
        batch =>
          logger.debug(Thread.currentThread().getName)
          batch
            .groupBy {
              case Row(leader, _) => leader
            }
            .mapValues(_.map(_.measure))
            .map {
              case (leader, measures) =>
                (leader, measures.map(SensorAggStat(_)).reduce(SensorAggStat.merge))
            }
      }

    val fileStats: Map[String, SensorAggStat] = batchStatIterator
      .reduceOption(SensorAggStat.mergeMaps).getOrElse(Map())

    fileStats
  }

  def generateOutput(numOfProcessedFiles: Int,
                     results: concurrent.Map[String, SensorAggStat]): String = {
    case class SensorFinalStat(leader: String, min: Int, avg: Long, max: Int, isNan: Boolean = false) {
      def mkString: String = {
        if (isNan) {
          s"$leader,$nan,$nan,$nan"
        } else {
          s"$leader,$min,$avg,$max"
        }
      }
    }
    val numOfSuccessMeasurements: Int = results.values.map(_.measureCount).sum
    val numOfFailedMeasurements: Int = results.values.map(_.nanCount).sum

    val messageHeader: String =
      s"""Num of processed files: ${numOfProcessedFiles}
         |Num of processed measurements: ${numOfSuccessMeasurements + numOfFailedMeasurements}
         |Num of failed measurements: ${numOfFailedMeasurements}
         |
         |Sensors with highest avg humidity:
         |
         |sensor-id,min,avg,max
         |""".stripMargin

    val orderedStat: List[SensorFinalStat] = results.toList.map {
      case (leader, SensorAggStat(min, max, sum, measureCount, _)) =>
        measureCount match {
          case 0 => SensorFinalStat(leader, 0, 0, 0, isNan = true)
          case _ => SensorFinalStat(leader, min, sum / measureCount, max)
        }
    }
      .sortBy(_.avg.unary_-)

    val messageBody: String = orderedStat
      .map(_.mkString)
      .reduce(_ + "\n" + _)

    messageHeader + messageBody
  }

  implicit def javaIteratorToScalaIterator[A](it: java.util.Iterator[A]): Iterator[A] = new Iterator[A] {
    override def hasNext: Boolean = it.hasNext

    override def next(): A = it.next()
  }

  def toJavaBiFunction[T, U, R](f: (T, U) => R): BiFunction[T, U, R] = new BiFunction[T, U, R] {
    override def apply(t: T, u: U): R = f(t, u)
  }
}
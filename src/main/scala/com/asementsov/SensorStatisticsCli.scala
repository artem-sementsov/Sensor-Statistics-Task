package com.asementsov

import java.io.File

import scopt.OptionParser

case class SensorStatisticsCli(inputDir: File = null)

object SensorStatisticsCli extends App {

  def apply(inputDirPath: String): SensorStatisticsCli = {
    new SensorStatisticsCli(new File(inputDirPath).getCanonicalFile)
  }

  def parseArgs(args: Seq[String]): SensorStatisticsCli = {
    val parser: OptionParser[SensorStatisticsCli] = new OptionParser[SensorStatisticsCli]("Sensor Statistics Task") {
      head("Sensor Statistics Task", "1.0.0")
      opt[String]('i', "inputDirPath")
        .required()
        .valueName("<inputDirPath>")
        .validate(value => {
          val inputDir: File = SensorStatisticsCli(value).inputDir
          if (!inputDir.exists()) {
            failure(s"Specified directory $value doesn't exist. Canonical path: ${inputDir.getCanonicalPath}")
          } else {
            success
          }
        })
        .action((value, config) => config.copy(inputDir = new File(value).getCanonicalFile))
        .text("Path to directory with input files")
      help("help").text("Show all possible options")
    }
    parser.parse(args, new SensorStatisticsCli) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException("Required parameters weren't specified")
    }
  }
}

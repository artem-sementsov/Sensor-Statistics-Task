package com.asementsov

import com.typesafe.scalalogging.LazyLogging
import com.univocity.parsers.common.record.Record

case class Row(leader: String, measure: String)

object Row extends LazyLogging {

  val headers: Seq[String] = Array("sensor-id", "humidity")

  val headerString: String = Row.headers.mkString(",")

  def apply(inputFileName: String, record: Record, idx: Int): Seq[Row] = {
    val rawString: String = record.getValues.mkString("'", ",", "'")
    logger.debug(s"Convert value $rawString to Row")

    val sensor: String = record.getString(headers.head)
    val humidity: String = record.getString(headers(1))

    if (sensor == null || humidity == null) {
      logger.warn(s"Not enough data to process row $rawString ($inputFileName:$idx)")
      Seq()
    } else {
      Seq(Row(sensor, humidity))
    }
  }
}

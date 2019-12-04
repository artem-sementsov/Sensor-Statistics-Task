package com.asementsov

import java.lang.Math.{max, min}

case class SensorAggStat(min: Int, max: Int, sum: Long, measureCount: Int, nanCount: Int)

object SensorAggStat {

  val minValue = 0
  val maxValue = 100
  val nan = "NaN"
  val nanStat = SensorAggStat(maxValue, minValue, 0, 0, 1)

  def singleMeasure(measure: Int) = SensorAggStat(measure, measure, measure, 1, 0)

  def apply(value: String): SensorAggStat = {
    value match {
      case noValue if noValue == nan => nanStat
      case _ => singleMeasure(value.toInt)
    }
  }

  def merge(left: SensorAggStat, right: SensorAggStat): SensorAggStat = {
    left match {
      case SensorAggStat(lMin, lMax, lSum, lMeasureCount, lNanCount) =>
        right match {
          case SensorAggStat(rMin, rMax, rSum, rMeasureCount, rNanCount) =>
            SensorAggStat(min(lMin, rMin), max(lMax, rMax), lSum + rSum, lMeasureCount + rMeasureCount, lNanCount + rNanCount)
        }
    }
  }

  def mergeMaps(left: Map[String, SensorAggStat], right: Map[String, SensorAggStat]): Map[String, SensorAggStat] = {
    val mergedMap: Map[String, List[SensorAggStat]] = (left.toList ++ right.toList)
      .groupBy(_._1)
      .mapValues(_.map(_._2))

    val mergedStat: Map[String, SensorAggStat] = mergedMap.map {
      case (leader, batchCalculations) => (leader, batchCalculations.reduce(merge))
    }
    mergedStat
  }
}
package com.asementsov

import com.asementsov.SensorAggStat._
import org.scalatest.{FlatSpec, Matchers}

class SensorAggStatSpec extends FlatSpec with Matchers {

  val zeroStat = SensorAggStat(0, 0, 0, 1, 0)
  val oneStat = SensorAggStat(1, 1, 1, 1, 0)

  it should "parse stat from string value properly" in {
    SensorAggStat(nan) shouldBe nanStat
    SensorAggStat("0") shouldBe zeroStat
    SensorAggStat("1") shouldBe oneStat
  }

  it should "join stats properly" in {
    merge(nanStat, nanStat) shouldBe nanStat.copy(nanCount = 2)
    merge(nanStat, zeroStat) shouldBe zeroStat.copy(nanCount = zeroStat.nanCount + 1)
    merge(zeroStat, nanStat) shouldBe zeroStat.copy(nanCount = zeroStat.nanCount + 1)
    merge(zeroStat, oneStat) shouldBe SensorAggStat(0, 1, 1, 2, 0)
  }
}

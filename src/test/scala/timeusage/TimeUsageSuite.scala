package timeusage

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random
import timeusage.TimeUsage
import timeusage.TimeUsageRow

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = TimeUsage

  test("regular expression") {
    val z: Seq[Char] = "Scala is Fun!!"
    var result:Boolean = false
    println(z)
    z match {
      case Seq('S','c','a','l','a', rest @ _*) =>
        println("rest is "+rest)
        result = true
      case Seq(_*) =>
        result = false
    }
    assert(result == true)

  }

  test("classify columns") {
    val result:(List[Column], List[Column], List[Column]) = testObject.classifiedColumns(List("t1805","t05","t010101", "t02", "t1801", "t18"))
    assert(result._1.size == 2)
    assert(result._1(0) == new Column("t010101"))

  }
}

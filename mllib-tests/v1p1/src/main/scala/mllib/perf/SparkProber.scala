package mllib.perf

import java.io.File

import scala.collection.mutable

import org.apache.spark.SparkContext

trait SparkProber {

  val listener = new ProbingListener
  private val recordAtTaskLevelForStage: mutable.HashSet[Int] = mutable.HashSet()

  def beforeProberJob(sc: SparkContext) = {
    listener.reset()
    listener.stageCnt = 0
    listener.stageCommunicationTime = 0
    listener.stageRuntime = 0
    sc.addSparkListener(listener)
  }

  def proberResults() = listener.proberResults

  def recordAtTaskLevelForStage(stage: Int): Unit = {
    recordAtTaskLevelForStage.add(stage)
    listener.recordAtTaskLevelForStage = recordAtTaskLevelForStage.toSet
  }

}

class ProberResults(var res: mutable.Map[String, String]) {

  def reset(): Unit = { res = mutable.HashMap.empty[String, String] }

  def print(headerMsg: String = "", sortByKey: Boolean = true) = {
    if (headerMsg.size > 0) {
      println(headerMsg)
    }
    if (sortByKey) {
      res.toSeq.sortBy(_._1).foreach { case (k, v) => println(k + ": " + v) }
    } else {
      res.foreach { case (k, v) => println(k + ": " + v) }
    }
    this
  }

  def printToFile(filePath: String, headerMsg: String = "", sortByKey: Boolean = true) = {

    def helper(f: java.io.File)(op: java.io.PrintWriter => Unit) {
      val p = new java.io.PrintWriter(f)
      try {
        op(p)
      } finally {
        p.flush()
        p.close()
      }
    }

    helper(new File(filePath)) { p =>
      if (headerMsg.size > 0) {
        p.println(headerMsg)
      }
      if (sortByKey) {
        res.toSeq.sortBy(_._1).foreach { case (k, v) => p.println(k + ": " + v) }
      } else {
        res.foreach { case (k, v) => p.println(k + ": " + v) }
      }
    }
    this
  }

  @inline
  def record(k: String, v: String): ProberResults = {
    res += (k -> v)
    this
  }

  def record(kvs: Map[String, String]): ProberResults = {
    res ++= kvs
    this
  }

  def timeStats(): ProberResults = {
    ProberResults(res.filter { case (k, v) => k.toLowerCase.matches(".*time") })
  }

  def copy(): ProberResults = ProberResults(res.clone())

}

object ProberResults {
  def apply(res: mutable.Map[String, String]): ProberResults = new ProberResults(res)
}

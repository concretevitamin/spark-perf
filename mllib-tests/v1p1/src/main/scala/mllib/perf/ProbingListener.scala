package mllib.perf

import org.apache.spark.Logging

import scala.collection.mutable

import org.apache.spark.scheduler._

/** A high-level wrapper class of SparkListener that reports various statistics. */
class ProbingListener extends SparkListener with Logging {

  val proberResults: ProberResults = ProberResults(mutable.HashMap.empty)

  // Chronological stage count (`stageCnt`)
  var recordAtTaskLevelForStage: Set[Int] = _

  var stageBeginTime: Long = 0
  var stageEndTime: Long = 0
  var stageCnt: Int = 0

  /** Fields for tasks within a stage. */
  var stageRuntime: Long = 0
  var stageCommunicationTime: Long = 0
  var stageEarliestTaskLaunchTime: Long = Long.MaxValue
  var stageLatestTaskEndTime: Long = -1

  var taskTimes = new mutable.ArrayBuffer[Long]

  def reset(): Unit = proberResults.reset()

  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    stageEarliestTaskLaunchTime =
      math.min(taskStart.taskInfo.launchTime, stageEarliestTaskLaunchTime)
  }

  // TODO: look into TaskMetrics#executorRunTime?
  // TODO: we should probably count shuffleWriteTime and shuffleReadTime for communication?
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskCommunicationTime = if (taskEnd.taskMetrics == null) {
      0L
    } else {
      taskEnd.taskMetrics.shuffleReadMetrics.map(_.fetchWaitTime).getOrElse(0L)
    }

    val taskComputationTime = taskEnd.taskInfo.duration - taskCommunicationTime
    val taskRunTime = taskCommunicationTime + taskComputationTime

    // total duration
    stageRuntime += taskEnd.taskInfo.duration
    // fetch wait time
    stageCommunicationTime += taskCommunicationTime

    stageLatestTaskEndTime = math.max(taskEnd.taskInfo.finishTime, stageLatestTaskEndTime)

    // Record at task level, for convenience onStageCompleted() will record
    // aggregated stats at stage level again.
    if (recordAtTaskLevelForStage != null && recordAtTaskLevelForStage(stageCnt + 1)) {
      val stageId = "stage-" + (stageCnt + 1)
      val taskId = "task-" + taskEnd.taskInfo.taskId
      val id = stageId + "-" + taskId

      proberResults.record(id + "-computationTimeRaw", taskComputationTime.toString)
      proberResults.record(id + "-communicationTimeRaw", taskCommunicationTime.toString)
      proberResults.record(id + "-totalTimeRaw", taskRunTime.toString)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    stageBeginTime = System.currentTimeMillis
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageInfo = stageCompleted.stageInfo

    stageEndTime = System.currentTimeMillis

    logInfo("Finished stage: " + stageInfo.stageId)
    stageCnt += 1

    // Executor (non-fetch) time plus other time
    val totalComputationTime = stageRuntime - stageCommunicationTime

    // use chronological stage id to avoid inversion
    val stageId = "stage-" + stageCnt

    // Stage runtime breakdown
    logInfo("Total stage runtime: " + ProbingListener.millisToString(stageRuntime))
    logInfo("Total communication runtime: " + ProbingListener.millisToString(stageCommunicationTime))
    logInfo("Total computation runtime: " + ProbingListener.millisToString(totalComputationTime))

    // Other info
    logInfo("Number of partitions: " + stageInfo.rddInfos.head.numPartitions)
    logInfo("Number of tasks: " + stageInfo.numTasks)

    proberResults.record(stageId + "-stageEndToEndTimeRaw", (stageEndTime - stageBeginTime).toString)
    proberResults.record(stageId + "-computationTimeRaw", totalComputationTime.toString)
    proberResults.record(stageId + "-communicationTimeRaw", stageCommunicationTime.toString)
    proberResults.record(stageId + "-totalTimeRaw", stageRuntime.toString)
    proberResults.record(stageId + "-numTasks", stageInfo.numTasks.toString)

    // For detailed break-down analysis
    proberResults.record(stageId + "-earliestTaskLaunchTime", stageEarliestTaskLaunchTime.toString)
    proberResults.record(stageId + "-latestTaskEndTime", stageLatestTaskEndTime.toString)

    stageRuntime = 0
    stageCommunicationTime = 0
    stageEarliestTaskLaunchTime = Long.MaxValue
    stageLatestTaskEndTime = -1
  }

}

object ProbingListener {
  // The below vals and method are cargo-culted from SparkListener.scala

  val seconds = 1000L
  val minutes = seconds * 60
  val hours = minutes * 60

  /**
   * reformat a time interval in milliseconds to a prettier format for output
   */
  def millisToString(ms: Long) = {
    val (size, units) =
      if (ms > hours) {
        (ms.toDouble / hours, "hours")
      } else if (ms > minutes) {
        (ms.toDouble / minutes, "min")
      } else if (ms > seconds) {
        (ms.toDouble / seconds, "s")
      } else {
        (ms.toDouble, "ms")
      }
    "%.1f %s".format(size, units)
  }

}

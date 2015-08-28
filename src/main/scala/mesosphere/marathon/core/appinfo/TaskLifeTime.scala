package mesosphere.marathon.core.appinfo

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.Timestamp

/**
  * Statistical information about task life times.
  *
  * The task life times are measured relative to the stagedAt time.
  */
case class TaskLifeTime(
  averageSeconds: Double,
  medianSeconds: Double)

object TaskLifeTime {
  def forSomeTasks(now: Timestamp, tasks: Iterable[MarathonTask]): Option[TaskLifeTime] = {
    def lifeTime(task: MarathonTask): Option[Double] = {
      if (task.hasStagedAt) {
        Some((now.toDateTime.getMillis - task.getStagedAt) / 1000.0)
      }
      else {
        None
      }
    }
    val lifeTimes = tasks.iterator.flatMap(lifeTime).toVector.sorted

    if (lifeTimes.isEmpty) {
      None
    }
    else {
      Some(
        TaskLifeTime(
          averageSeconds = lifeTimes.sum / lifeTimes.size,
          medianSeconds = lifeTimes(lifeTimes.size / 2)
        )
      )
    }
  }
}


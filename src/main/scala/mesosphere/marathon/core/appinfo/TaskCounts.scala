package mesosphere.marathon.core.appinfo

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.health.Health
import org.apache.mesos.{ Protos => mesos }

/**
  * @param tasksStaged snapshot of the number of staged tasks
  * @param tasksRunning snapshot of the number of running tasks
  * @param tasksHealthy snapshot of the number of healthy tasks (does not include tasks without health info)
  * @param tasksUnhealthy snapshot of the number of unhealthy tasks (does not include tasks without health info)
  */
case class TaskCounts(
  tasksStaged: Int,
  tasksRunning: Int,
  tasksHealthy: Int,
  tasksUnhealthy: Int)

object TaskCounts {
  def zero: TaskCounts = TaskCounts(tasksStaged = 0, tasksRunning = 0, tasksHealthy = 0, tasksUnhealthy = 0)

  def apply(appTasks: Iterable[MarathonTask], statuses: Map[String, Seq[Health]]): TaskCounts = {
    def hasStatus(state: mesos.TaskState)(task: MarathonTask): Boolean =
      task.hasStatus && task.getStatus.getState == state
    def isHealthy(task: MarathonTask): Boolean =
      statuses.get(task.getId).exists { healths => healths.nonEmpty && healths.forall(_.alive) }
    def isUnhealthy(task: MarathonTask): Boolean = statuses.get(task.getId).exists(_.exists(!_.alive))

    TaskCounts(
      tasksStaged = appTasks.count(hasStatus(mesos.TaskState.TASK_STAGING)),
      tasksRunning = appTasks.count(hasStatus(mesos.TaskState.TASK_RUNNING)),
      tasksHealthy = appTasks.count(isHealthy),
      tasksUnhealthy = appTasks.count(isUnhealthy)
    )
  }
}

package mesosphere.marathon.core.appinfo

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.health.Health
import mesosphere.marathon.state.AppDefinition.VersionInfo
import mesosphere.marathon.state.AppDefinition.VersionInfo.FullVersionInfo
import mesosphere.marathon.state.Timestamp

case class TaskStatsByVersion(
  maybeStartedAfterLastScaling: Option[TaskStats],
  maybeWithLatestConfig: Option[TaskStats],
  maybeWithOutdatedConfig: Option[TaskStats],
  maybeTotalSummary: Option[TaskStats])

object TaskStatsByVersion {
  def apply(
    now: Timestamp,
    versionInfo: VersionInfo,
    tasks: Iterable[MarathonTask],
    statuses: Map[String, Seq[Health]]): TaskStatsByVersion =
    {

      def statsForVersion(versionTest: Long => Boolean): Option[TaskStats] = {
        def byVersion(task: MarathonTask): Boolean = {
          // argh, parsing the data again and again?
          versionTest(Timestamp(task.getVersion).toDateTime.getMillis)
        }
        TaskStats.forSomeTasks(now, tasks.filter(byVersion), statuses)
      }

      val maybeFullVersionInfo = versionInfo match {
        case full: FullVersionInfo => Some(full)
        case _                     => None
      }

      TaskStatsByVersion(
        maybeTotalSummary = TaskStats.forSomeTasks(now, tasks, statuses),
        maybeStartedAfterLastScaling = maybeFullVersionInfo.flatMap { vi =>
          statsForVersion(_ >= vi.lastScalingAt.toDateTime.getMillis)
        },
        maybeWithLatestConfig = maybeFullVersionInfo.flatMap { vi =>
          statsForVersion(_ >= vi.lastConfigChangeAt.toDateTime.getMillis)
        },
        maybeWithOutdatedConfig = maybeFullVersionInfo.flatMap { vi =>
          statsForVersion(_ < vi.lastConfigChangeAt.toDateTime.getMillis)
        }
      )
    }
}

case class TaskStats(
  counts: TaskCounts,
  lifeTime: TaskLifeTime)

object TaskStats {
  def forSomeTasks(
    now: Timestamp, tasks: Iterable[MarathonTask], statuses: Map[String, Seq[Health]]): Option[TaskStats] = {
    TaskLifeTime.forSomeTasks(now, tasks).map { lifeTime =>
      TaskStats(
        counts = TaskCounts(tasks, statuses),
        lifeTime = lifeTime
      )
    }
  }
}

package mesosphere.marathon.core.appinfo

import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.health.Health
import mesosphere.marathon.state.{ AppDefinition, Timestamp }
import mesosphere.marathon.{ Protos, MarathonSpec }
import org.scalatest.{ Matchers, GivenWhenThen }
import org.apache.mesos.{ Protos => mesos }
import play.api.libs.json.Json
import scala.concurrent.duration._

class TaskStatsByVersionTest extends MarathonSpec with GivenWhenThen with Matchers {

  test("no tasks") {
    Given("no tasks")
    When("calculating stats")
    val stats = TaskStatsByVersion(
      now = now,
      versionInfo = versionInfo,
      tasks = Seq.empty,
      statuses = Map.empty[String, Seq[Health]]
    )
    Then("we get none")
    stats should be (
      TaskStatsByVersion(
        maybeStartedAfterLastScaling = None,
        maybeWithLatestConfig = None,
        maybeWithOutdatedConfig = None,
        maybeTotalSummary = None
      )
    )
  }

  test("tasks are correctly split along categories") {
    Given("various tasks")
    taskIdCounter = 0
    val outdatedTasks = Vector(
      runningTaskStagedAt(outdatedVersion, 1.seconds),
      runningTaskStagedAt(outdatedVersion, 2.seconds)
    )
    val afterLastScalingTasks = Vector(
      runningTaskStagedAt(lastScalingAt, 1.seconds),
      runningTaskStagedAt(lastScalingAt, 2.seconds)
    )
    val afterLastConfigChangeTasks = Vector(
      runningTaskStagedAt(lastConfigChangeAt, 1.seconds),
      runningTaskStagedAt(intermediaryScalingAt, 2.seconds)
    ) ++ afterLastScalingTasks

    val tasks = outdatedTasks ++ afterLastConfigChangeTasks
    val statuses = Map.empty[String, Seq[Health]]

    When("calculating stats")
    val stats = TaskStatsByVersion(
      now = now,
      versionInfo = versionInfo,
      tasks = tasks,
      statuses = statuses
    )
    Then("we get the correct stats")
    import mesosphere.marathon.api.v2.json.Formats._
    withClue(Json.prettyPrint(Json.obj("stats" -> stats, "tasks" -> tasks))) {
      stats.maybeWithOutdatedConfig should not be empty
      stats.maybeWithLatestConfig should not be empty
      stats.maybeStartedAfterLastScaling should not be empty
      stats.maybeTotalSummary should not be empty

      stats.maybeWithOutdatedConfig should be (TaskStats.forSomeTasks(now, outdatedTasks, statuses))
      stats.maybeWithLatestConfig should be (TaskStats.forSomeTasks(now, afterLastConfigChangeTasks, statuses))
      stats.maybeStartedAfterLastScaling should be (TaskStats.forSomeTasks(now, afterLastScalingTasks, statuses))
      stats.maybeTotalSummary should be (TaskStats.forSomeTasks(now, tasks, statuses))

      stats should be (
        TaskStatsByVersion(
          maybeStartedAfterLastScaling = TaskStats.forSomeTasks(now, afterLastScalingTasks, statuses),
          maybeWithLatestConfig = TaskStats.forSomeTasks(now, afterLastConfigChangeTasks, statuses),
          maybeWithOutdatedConfig = TaskStats.forSomeTasks(now, outdatedTasks, statuses),
          maybeTotalSummary = TaskStats.forSomeTasks(now, tasks, statuses)
        )
      )
    }

  }

  private[this] val now: Timestamp = ConstantClock().now()
  private val lastScalingAt: Timestamp = now - 10.seconds
  private val intermediaryScalingAt: Timestamp = now - 20.seconds
  private val lastConfigChangeAt: Timestamp = now - 100.seconds
  private val outdatedVersion: Timestamp = now - 200.seconds
  private[this] val versionInfo = AppDefinition.VersionInfo.FullVersionInfo(
    version = lastScalingAt,
    lastScalingAt = lastScalingAt,
    lastConfigChangeAt = lastConfigChangeAt
  )
  private[this] var taskIdCounter = 0
  private[this] def createTask(): Protos.MarathonTask.Builder = {
    taskIdCounter += 1
    Protos.MarathonTask
      .newBuilder()
      .setId(s"task$taskIdCounter")
  }

  private[this] def statusForState(state: mesos.TaskState): mesos.TaskStatus = {
    mesos.TaskStatus
      .newBuilder()
      .setState(state)
      .buildPartial()
  }

  private[this] def unstagedTask(): Protos.MarathonTask = {
    createTask().build()
  }

  private[this] def stagedTaskAt(version: Timestamp, stagingDelay: FiniteDuration): Protos.MarathonTask = {
    val stagedAt = (version + stagingDelay).toDateTime.getMillis
    createTask()
      .setStatus(statusForState(mesos.TaskState.TASK_STAGING))
      .setVersion(version.toString)
      .setStagedAt(stagedAt)
      .buildPartial()
  }

  private[this] def runningTaskStagedAt(version: Timestamp, stagingDelay: FiniteDuration): Protos.MarathonTask = {
    val stagedAt = (version + stagingDelay).toDateTime.getMillis
    createTask()
      .setStatus(statusForState(mesos.TaskState.TASK_RUNNING))
      .setVersion(version.toString)
      .setStagedAt(stagedAt)
      .buildPartial()
  }
}

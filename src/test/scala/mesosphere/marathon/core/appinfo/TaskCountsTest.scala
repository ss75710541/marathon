package mesosphere.marathon.core.appinfo

import mesosphere.marathon.health.Health
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.{ MarathonSpec, Protos }
import mesosphere.util.Mockito
import org.apache.mesos.{ Protos => mesos }
import org.scalatest.{ GivenWhenThen, Matchers }

class TaskCountsTest extends MarathonSpec with GivenWhenThen with Mockito with Matchers {
  test("count no tasks") {
    When("getting counts for no tasks")
    val counts = TaskCounts(appTasks = Seq.empty, statuses = Map.empty)
    Then("all counts are zero")
    counts should be(TaskCounts.zero)
  }

  test("one unstaged task") {
    Given("one unstaged task")
    val oneUnstagedTask = Seq(
      unstagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneUnstagedTask, statuses = Map.empty)
    Then("all counts are 0")
    counts should be(TaskCounts.zero)
  }

  test("one staged task") {
    Given("one staged task")
    val oneStagedTask = Seq(
      stagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneStagedTask, statuses = Map.empty)
    Then("all counts are 0 except staged")
    counts should be(TaskCounts.zero.copy(tasksStaged = 1))
  }

  test("one running task") {
    Given("one running task")
    val oneRunningTask = Seq(
      runningTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneRunningTask, statuses = Map.empty)
    Then("all counts are 0 except running")
    counts should be(TaskCounts.zero.copy(tasksRunning = 1))
  }

  test("one healthy task") {
    Given("one unstaged task with alive Health")
    val oneRunningTask = Seq(
      unstagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneRunningTask, statuses = Map("task1" -> aliveHealth))
    Then("all counts are 0 except healthy")
    counts should be(TaskCounts.zero.copy(tasksHealthy = 1))
  }

  test("one unhealthy task") {
    Given("one unstaged task with !alive health")
    val oneRunningTask = Seq(
      unstagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneRunningTask, statuses = Map("task1" -> notAliveHealth))
    Then("all counts are 0 except tasksUnhealthy")
    counts should be(TaskCounts.zero.copy(tasksUnhealthy = 1))
  }

  test("a task with mixed health is counted as unhealthy") {
    Given("one task with mixed health")
    val oneRunningTask = Seq(
      unstagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneRunningTask, statuses = Map("task1" -> mixedHealth))
    Then("all counts are 0 except tasksUnhealthy")
    counts should be(TaskCounts.zero.copy(tasksUnhealthy = 1))
  }

  test("one unstaged task with empty health is not counted") {
    Given("one unstaged task with empty health info")
    val oneRunningTask = Seq(
      unstagedTask("task1")
    )
    When("getting counts")
    val counts = TaskCounts(appTasks = oneRunningTask, statuses = Map("task1" -> noHealths))
    Then("all counts are 0")
    counts should be(TaskCounts.zero)
  }

  private[this] val noHealths = Seq.empty[Health]
  private[this] val aliveHealth = Seq(Health("task1", lastSuccess = Some(Timestamp(1))))
  require(aliveHealth.forall(_.alive))
  private[this] val notAliveHealth = Seq(Health("task1", lastFailure = Some(Timestamp(1))))
  require(notAliveHealth.forall(!_.alive))
  private[this] val mixedHealth = aliveHealth ++ notAliveHealth

  private[this] def statusForState(state: mesos.TaskState): mesos.TaskStatus = {
    mesos.TaskStatus
      .newBuilder()
      .setState(state)
      .buildPartial()
  }

  private[this] def unstagedTask(id: String): Protos.MarathonTask = {
    Protos.MarathonTask
      .newBuilder()
      .setId(id)
      .buildPartial()
  }

  private[this] def stagedTask(id: String): Protos.MarathonTask = {
    Protos.MarathonTask
      .newBuilder()
      .setId(id)
      .setStatus(statusForState(mesos.TaskState.TASK_STAGING))
      .buildPartial()
  }

  private[this] def runningTask(id: String): Protos.MarathonTask = {
    Protos.MarathonTask
      .newBuilder()
      .setId(id)
      .setStatus(statusForState(mesos.TaskState.TASK_RUNNING))
      .buildPartial()
  }

}

package mesosphere.marathon.core.appinfo

import mesosphere.marathon.{ MarathonSpec, Protos }
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.state.Timestamp
import mesosphere.util.Mockito
import org.scalatest.{ GivenWhenThen, Matchers }

class TaskLifeTimeTest extends MarathonSpec with Mockito with GivenWhenThen with Matchers {
  private[this] val now: Timestamp = ConstantClock().now()
  private[this] var taskIdCounter = 0
  private[this] def createTask(): Protos.MarathonTask.Builder = {
    taskIdCounter += 1
    Protos.MarathonTask
      .newBuilder()
      .setId(s"task$taskIdCounter")
  }

  private[this] def unstagedTask(): Protos.MarathonTask = {
    createTask().build()
  }

  private[this] def stagedTaskWithLifeTime(lifeTimeSeconds: Double): Protos.MarathonTask = {
    val stagedAt = (now.toDateTime.getMillis - lifeTimeSeconds * 1000.0).round
    createTask().setStagedAt(stagedAt).build()
  }

  test("life time for no tasks") {
    Given("no tasks")
    When("calculating life times")
    val lifeTimes = TaskLifeTime.forSomeTasks(now, Seq.empty)
    Then("we get none")
    lifeTimes should be(None)
  }

  test("life time only for tasks which have not yet been staged") {
    Given("unstaged tasks")
    val tasks = (1 to 3).map(_ => unstagedTask())
    When("calculating life times")
    val lifeTimes = TaskLifeTime.forSomeTasks(now, tasks)
    Then("we get none")
    lifeTimes should be(None)
  }

  test("life times for task with life times") {
    Given("three tasks with the life times 2s, 4s, 9s")
    val tasks = Seq(2.0, 4.0, 9.0).map(stagedTaskWithLifeTime)
    When("calculating life times")
    val lifeTimes = TaskLifeTime.forSomeTasks(now, tasks)
    Then("we get the correct stats")
    lifeTimes should be(
      Some(
        TaskLifeTime(
          averageSeconds = 5.0,
          medianSeconds = 4.0
        )
      )
    )
  }

  test("life times for task with life times ignore unstaged") {
    Given("three tasks with the life times 2s, 4s, 9s")
    val tasks = Seq(2.0, 4.0, 9.0).map(stagedTaskWithLifeTime) ++ Seq(unstagedTask())
    When("calculating life times")
    val lifeTimes = TaskLifeTime.forSomeTasks(now, tasks)
    Then("we get the correct stats")
    lifeTimes should be(
      Some(
        TaskLifeTime(
          averageSeconds = 5.0,
          medianSeconds = 4.0
        )
      )
    )
  }
}

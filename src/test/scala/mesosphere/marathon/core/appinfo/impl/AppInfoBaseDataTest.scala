package mesosphere.marathon.core.appinfo.impl

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.appinfo.{ TaskStatsByVersion, TaskStats, AppInfo, EnrichedTask, TaskCounts }
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.health.{ Health, HealthCheckManager }
import mesosphere.marathon.state._
import mesosphere.marathon.tasks.TaskTracker
import mesosphere.marathon.upgrade.DeploymentManager.DeploymentStepInfo
import mesosphere.marathon.upgrade.{ DeploymentPlan, DeploymentStep }
import mesosphere.marathon.{ MarathonSchedulerService, MarathonSpec }
import mesosphere.util.Mockito
import org.apache.mesos.Protos
import org.scalatest.{ GivenWhenThen, Matchers }
import play.api.libs.json.Json

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class AppInfoBaseDataTest extends MarathonSpec with GivenWhenThen with Mockito with Matchers {
  import org.scalatest.concurrent.ScalaFutures._

  class Fixture {
    lazy val clock = ConstantClock()
    lazy val taskTracker = mock[TaskTracker]
    lazy val healthCheckManager = mock[HealthCheckManager]
    lazy val marathonSchedulerService = mock[MarathonSchedulerService]
    lazy val taskFailureRepository = mock[TaskFailureRepository]

    lazy val baseData = new AppInfoBaseData(
      clock,
      taskTracker,
      healthCheckManager,
      marathonSchedulerService,
      taskFailureRepository
    )

    def verifyNoMoreInteractions(): Unit = {
      noMoreInteractions(taskTracker)
      noMoreInteractions(healthCheckManager)
      noMoreInteractions(marathonSchedulerService)
      noMoreInteractions(taskFailureRepository)
    }
  }

  val app = AppDefinition(PathId("/test"))
  val other = AppDefinition(PathId("/other"))

  test("not embedding anything results in no calls") {
    val f = new Fixture

    When("getting AppInfos without embeds")
    val appInfo = f.baseData.appInfoFuture(app, Set.empty).futureValue

    Then("we get an empty appInfo")
    appInfo should be(AppInfo(app))

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting tasks retrieves tasks from taskTracker and health infos") {
    val f = new Fixture
    Given("three tasks in the task tracker")
    val running1 = MarathonTask
      .newBuilder()
      .setId("task1")
      .setStatus(Protos.TaskStatus.newBuilder().setState(Protos.TaskState.TASK_RUNNING).buildPartial())
      .buildPartial()
    val running2 = running1.toBuilder.setId("task2").buildPartial()
    val running3 = running1.toBuilder.setId("task3").buildPartial()

    f.taskTracker.get(app.id) returns Set(running1, running2, running3)

    val alive = Health("task2", lastSuccess = Some(Timestamp(1)))
    val unhealthy = Health("task3", lastFailure = Some(Timestamp(1)))

    f.healthCheckManager.statuses(app.id) returns Future.successful(
      Map(
        running1.getId -> Seq.empty,
        running2.getId -> Seq(alive),
        running3.getId -> Seq(unhealthy)
      )
    )

    When("requesting AppInfos with tasks")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.Tasks)).futureValue

    Then("we get a tasks object in the appInfo")
    appInfo.maybeTasks should not be empty
    appInfo.maybeTasks.get.map(_.appId.toString) should have size (3)
    appInfo.maybeTasks.get.map(_.task.getId).toSet should be (Set("task1", "task2", "task3"))

    appInfo should be(AppInfo(app, maybeTasks = Some(
      Seq(
        EnrichedTask(app.id, running1, Seq.empty),
        EnrichedTask(app.id, running2, Seq(alive)),
        EnrichedTask(app.id, running3, Seq(unhealthy))
      )
    )))

    And("the taskTracker should have been called")
    verify(f.taskTracker, times(1)).get(app.id)

    And("the healthCheckManager as well")
    verify(f.healthCheckManager, times(1)).statuses(app.id)

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting task counts only retrieves tasks from taskTracker and health stats") {
    val f = new Fixture
    Given("one staged and two running tasks in the taskTracker")
    val staged = MarathonTask
      .newBuilder()
      .setId("task1")
      .setStatus(Protos.TaskStatus.newBuilder().setState(Protos.TaskState.TASK_STAGING).buildPartial())
      .buildPartial()
    val running = MarathonTask
      .newBuilder()
      .setId("task2")
      .setStatus(Protos.TaskStatus.newBuilder().setState(Protos.TaskState.TASK_RUNNING).buildPartial())
      .buildPartial()
    val running2 = running.toBuilder.setId("task3").buildPartial()

    f.taskTracker.get(app.id) returns Set(staged, running, running2)

    f.healthCheckManager.statuses(app.id) returns Future.successful(
      Map(
        "task1" -> Seq(),
        "task2" -> Seq(Health("task2", lastFailure = Some(Timestamp(1)))),
        "task3" -> Seq(Health("task3", lastSuccess = Some(Timestamp(2))))
      )
    )

    When("requesting AppInfos with counts")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.Counts)).futureValue

    Then("we get counts object in the appInfo")
    appInfo should be(AppInfo(app, maybeCounts = Some(
      TaskCounts(tasksStaged = 1, tasksRunning = 2, tasksHealthy = 1, tasksUnhealthy = 1)
    )))

    And("the taskTracker should have been called")
    verify(f.taskTracker, times(1)).get(app.id)

    And("the healthCheckManager as well")
    verify(f.healthCheckManager, times(1)).statuses(app.id)

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting deployments does not request anything else") {
    val f = new Fixture
    Given("One related and one unrelated deployment")
    val emptyGroup = Group.empty
    val relatedDeployment = DeploymentPlan(emptyGroup, emptyGroup.copy(apps = Set(app)))
    val unrelatedDeployment = DeploymentPlan(emptyGroup, emptyGroup.copy(apps = Set(other)))
    f.marathonSchedulerService.listRunningDeployments() returns Future.successful(Seq[DeploymentStepInfo](
      DeploymentStepInfo(relatedDeployment, DeploymentStep(Seq.empty), 1),
      DeploymentStepInfo(unrelatedDeployment, DeploymentStep(Seq.empty), 1)
    ))

    When("Getting AppInfos without counts")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.Deployments)).futureValue

    Then("we get an counts in the appInfo")
    appInfo should be(AppInfo(app, maybeDeployments = Some(
      Seq(Identifiable(relatedDeployment.id))
    )))

    And("the marathonSchedulerService should have been called to retrieve the deployments")
    verify(f.marathonSchedulerService, times(1)).listRunningDeployments()

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting deployments does work if no deployments are running") {
    val f = new Fixture
    Given("No deployments")
    f.marathonSchedulerService.listRunningDeployments() returns Future.successful(
      Seq.empty[DeploymentStepInfo]
    )

    When("Getting AppInfos with deployments")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.Deployments)).futureValue

    Then("we get an empty list of deployments")
    appInfo should be(AppInfo(app, maybeDeployments = Some(
      Seq.empty
    )))

    And("the marathonSchedulerService should have been called to retrieve the deployments")
    verify(f.marathonSchedulerService, times(1)).listRunningDeployments()

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting lastTaskFailure when one exists") {
    val f = new Fixture
    Given("One last taskFailure")
    f.taskFailureRepository.current(app.id) returns Future.successful(Some(TaskFailureTestHelper.taskFailure))

    When("Getting AppInfos with last task failures")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.LastTaskFailure)).futureValue

    Then("we get the failure in the app info")
    appInfo should be(AppInfo(app, maybeLastTaskFailure = Some(
      TaskFailureTestHelper.taskFailure
    )))

    And("the taskFailureRepository should have been called to retrieve the failure")
    verify(f.taskFailureRepository, times(1)).current(app.id)

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting lastTaskFailure when None exist") {
    val f = new Fixture
    Given("no taskFailure")
    f.taskFailureRepository.current(app.id) returns Future.successful(None)

    When("Getting AppInfos with last task failures")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.LastTaskFailure)).futureValue

    Then("we get no failure in the app info")
    appInfo should be(AppInfo(app))

    And("the taskFailureRepository should have been called to retrieve the failure")
    verify(f.taskFailureRepository, times(1)).current(app.id)

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("requesting taskStats") {
    val f = new Fixture
    Given("one staged and two running tasks in the taskTracker")
    val staged = MarathonTask
      .newBuilder()
      .setId("task1")
      .setStatus(Protos.TaskStatus.newBuilder().setState(Protos.TaskState.TASK_STAGING).buildPartial())
      .setStagedAt((f.clock.now() - 10.seconds).toDateTime.getMillis)
      .buildPartial()
    val running = MarathonTask
      .newBuilder()
      .setId("task2")
      .setStatus(Protos.TaskStatus.newBuilder().setState(Protos.TaskState.TASK_RUNNING).buildPartial())
      .setStagedAt((f.clock.now() - 11.seconds).toDateTime.getMillis)
      .buildPartial()
    val running2 = running.toBuilder.setId("task3").buildPartial()

    val tasks: Set[MarathonTask] = Set(staged, running, running2)
    f.taskTracker.get(app.id) returns tasks

    val statuses: Map[String, Seq[Health]] = Map(
      "task1" -> Seq(),
      "task2" -> Seq(Health("task2", lastFailure = Some(Timestamp(1)))),
      "task3" -> Seq(Health("task3", lastSuccess = Some(Timestamp(2))))
    )
    f.healthCheckManager.statuses(app.id) returns Future.successful(statuses)

    When("requesting AppInfos with taskStats")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.TaskStats)).futureValue

    Then("we get taskStats object in the appInfo")
    // we check the calculation of the stats in TaskStatsByVersionTest, so we only check some basic stats

    import mesosphere.marathon.api.v2.json.Formats._
    withClue(Json.prettyPrint(Json.toJson(appInfo))) {
      appInfo.maybeTaskStats should not be empty
      appInfo.maybeTaskStats.get.maybeTotalSummary should not be empty
      appInfo.maybeTaskStats.get.maybeTotalSummary.get.counts.tasksStaged should be (1)
      appInfo.maybeTaskStats.get.maybeTotalSummary.get.counts.tasksRunning should be (2)

      appInfo should be(AppInfo(
        app,
        maybeTaskStats = Some(TaskStatsByVersion(f.clock.now(), app.versionInfo, tasks, statuses))
      ))
    }

    And("the taskTracker should have been called")
    verify(f.taskTracker, times(1)).get(app.id)

    And("the healthCheckManager as well")
    verify(f.healthCheckManager, times(1)).statuses(app.id)

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("Combining embed options work") {
    val f = new Fixture
    Given("One last taskFailure and no deployments")
    f.taskFailureRepository.current(app.id) returns Future.successful(Some(TaskFailureTestHelper.taskFailure))
    f.marathonSchedulerService.listRunningDeployments() returns Future.successful(
      Seq.empty[DeploymentStepInfo]
    )

    When("Getting AppInfos with last task failures and deployments")
    val appInfo = f.baseData.appInfoFuture(app, Set(AppInfo.Embed.LastTaskFailure, AppInfo.Embed.Deployments)).futureValue

    Then("we get the failure in the app info")
    appInfo should be(AppInfo(
      app,
      maybeLastTaskFailure = Some(TaskFailureTestHelper.taskFailure),
      maybeDeployments = Some(Seq.empty)
    ))

    And("the taskFailureRepository should have been called to retrieve the failure")
    verify(f.taskFailureRepository, times(1)).current(app.id)

    And("the marathonSchedulerService should have been called to retrieve the deployments")
    verify(f.marathonSchedulerService, times(1)).listRunningDeployments()

    And("we have no more interactions")
    f.verifyNoMoreInteractions()
  }

}

package mesosphere.marathon.api

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.{ AppDefinition, Group, GroupManager, PathId, Timestamp }
import mesosphere.marathon.tasks.{ MarathonTasks, TaskTracker }
import mesosphere.marathon.upgrade.DeploymentPlan
import mesosphere.marathon.{ AppLockedException, MarathonSchedulerService, MarathonSpec, UnknownAppException }
import mesosphere.mesos.protos.Implicits.slaveIDToProto
import mesosphere.mesos.protos.SlaveID
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfterAll, Matchers }

import scala.concurrent.Future

class TaskKillerTest extends MarathonSpec
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with ScalaFutures {

  var tracker: TaskTracker = _
  var service: MarathonSchedulerService = _
  var groupManager: GroupManager = _
  var taskKiller: TaskKiller = _

  before {
    service = mock[MarathonSchedulerService]
    tracker = mock[TaskTracker]
    groupManager = mock[GroupManager]
    taskKiller = new TaskKiller(tracker, groupManager, service)
  }

  test("AppNotFound") {
    val appId = PathId("invalid")
    when(tracker.contains(appId)).thenReturn(false)

    val result = taskKiller.kill(appId, (tasks) => Set.empty[MarathonTask], force = true)
    result.failed.futureValue shouldEqual UnknownAppException(appId)
  }

  test("AppNotFound with scaling") {
    val appId = PathId("invalid")
    when(tracker.contains(appId)).thenReturn(false)

    val result = taskKiller.killAndScale(appId, (tasks) => Set.empty[MarathonTask], force = true)
    result.failed.futureValue shouldEqual UnknownAppException(appId)
  }

  test("KillRequested with scaling") {
    val appId = PathId(List("my", "app"))
    val now = Timestamp.now()
    val task1 = MarathonTasks.makeTask(
      "task-1", "host", ports = Nil, attributes = Nil, version = now, now = now,
      slaveId = SlaveID("1")
    )
    val task2 = MarathonTasks.makeTask(
      "task-2", "host", ports = Nil, attributes = Nil, version = Timestamp.now(), now = now,
      slaveId = SlaveID("1")
    )
    val tasksToKill = Set(task1, task2)
    val originalAppDefinition = AppDefinition(appId, instances = 23)

    when(tracker.contains(appId)).thenReturn(true)
    when(groupManager.group(appId.parent)).thenReturn(Future.successful(Some(Group.emptyWithId(appId.parent))))

    val appIdCaptor = ArgumentCaptor.forClass(classOf[PathId])
    val appUpdateCaptor = ArgumentCaptor.forClass(classOf[(Option[AppDefinition]) => AppDefinition])
    val forceCaptor = ArgumentCaptor.forClass(classOf[Boolean])
    val toKillCaptor = ArgumentCaptor.forClass(classOf[Set[MarathonTask]])
    val expectedDeploymentPlan = DeploymentPlan.empty
    when(groupManager.updateApp(
      appId = appIdCaptor.capture(),
      fn = appUpdateCaptor.capture(),
      version = any[Timestamp](),
      force = forceCaptor.capture(),
      toKill = toKillCaptor.capture())
    ).thenReturn(Future.successful(expectedDeploymentPlan))

    val result = taskKiller.killAndScale(appId, (tasks) => tasksToKill, force = true)
    result.futureValue shouldEqual expectedDeploymentPlan
    appIdCaptor.getValue shouldEqual appId
    appUpdateCaptor.getValue.apply(Some(originalAppDefinition)) shouldEqual originalAppDefinition.copy(instances = 21)
    forceCaptor.getValue shouldEqual true
    toKillCaptor.getValue shouldEqual tasksToKill
  }

  test("KillRequested without scaling") {
    val appId = PathId(List("my", "app"))
    val tasksToKill = Set(MarathonTask.getDefaultInstance)
    when(tracker.contains(appId)).thenReturn(true)

    val result = taskKiller.kill(appId, (tasks) => tasksToKill, force = true)
    result.futureValue shouldEqual tasksToKill
    verify(service, times(1)).killTasks(appId, tasksToKill)
    verifyNoMoreInteractions(groupManager)
  }

  test("Kill and scale w/o force should fail if there is a deployment") {
    val appId = PathId(List("my", "app"))
    val now = Timestamp.now()
    val task1 = MarathonTasks.makeTask(
      "task-1", "host", ports = Nil, attributes = Nil, version = Timestamp.now(), now = now,
      slaveId = SlaveID("1")
    )
    val task2 = MarathonTasks.makeTask(
      "task-2", "host", ports = Nil, attributes = Nil, version = Timestamp.now(), now = now,
      slaveId = SlaveID("1")
    )
    val tasksToKill = Set(task1, task2)

    when(tracker.contains(appId)).thenReturn(true)
    when(groupManager.group(appId.parent)).thenReturn(Future.successful(Some(Group.emptyWithId(appId.parent))))
    val forceCaptor = ArgumentCaptor.forClass(classOf[Boolean])
    when(groupManager.updateApp(any(), any(), any(), forceCaptor.capture(), any())
    ).thenReturn(Future.failed(AppLockedException()))

    val result = taskKiller.killAndScale(appId, (tasks) => tasksToKill, force = false)
    forceCaptor.getValue shouldEqual false
    result.failed.futureValue shouldEqual AppLockedException()
  }

}

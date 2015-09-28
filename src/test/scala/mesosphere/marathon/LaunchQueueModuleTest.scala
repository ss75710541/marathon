package mesosphere.marathon

import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.core.base.{ Clock, ShutdownHooks }
import mesosphere.marathon.core.launchqueue.{ LaunchQueueConfig, LaunchQueueModule }
import mesosphere.marathon.core.leadership.AlwaysElectedLeadershipModule
import mesosphere.marathon.core.matcher.DummyOfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskBusModule
import mesosphere.marathon.integration.setup.WaitTestSupport
import mesosphere.marathon.state.{ AppRepository, PathId }
import mesosphere.marathon.tasks.TaskFactory.CreatedTask
import mesosphere.marathon.tasks.{ TaskFactory, TaskIdUtil, TaskTracker }
import org.apache.mesos.Protos.TaskID
import org.hamcrest.{ BaseMatcher, Description }
import org.mockito.Matchers
import org.mockito.Mockito.{ when => call, _ }
import org.mockito.internal.matchers.Equality
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }

import scala.concurrent.Await
import scala.concurrent.duration._

class LaunchQueueModuleTest extends MarathonSpec with BeforeAndAfter with GivenWhenThen {

  test("empty queue returns no results") {
    When("querying queue")
    val apps = taskQueue.list

    Then("no apps are returned")
    assert(apps.isEmpty)
  }

  test("An added queue item is returned in list") {
    Given("a task queue with one item")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    taskQueue.add(app)

    When("querying its contents")
    val list = taskQueue.list

    Then("we get back the added app")
    assert(list.size == 1)
    assert(list.head.app == app)
    assert(list.head.tasksLeftToLaunch == 1)
    assert(list.head.tasksLaunchedOrRunning == 0)
    assert(list.head.taskLaunchesInFlight == 0)
    verify(taskTracker).getTasks(app.id)
  }

  test("An added queue item is reflected via count") {
    Given("a task queue with one item")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    taskQueue.add(app)

    When("querying its count")
    val count = taskQueue.count(app.id)

    Then("we get a count == 1")
    assert(count == 1)
    verify(taskTracker).getTasks(app.id)
  }

  test("A purged queue item has a count of 0") {
    Given("a task queue with one item which is purged")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    taskQueue.add(app)
    taskQueue.purge(app.id)

    When("querying its count")
    val count = taskQueue.count(app.id)

    Then("we get a count == 0")
    assert(count == 0)
    verify(taskTracker).getTasks(app.id)
  }

  test("A re-added queue item has a count of 1") {
    Given("a task queue with one item which is purged")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    taskQueue.add(app)
    taskQueue.purge(app.id)
    taskQueue.add(app)

    When("querying its count")
    val count = taskQueue.count(app.id)

    Then("we get a count == 1")
    assert(count == 1)
    verify(taskTracker, times(2)).getTasks(app.id)
  }

  test("adding a queue item registers new offer matcher") {
    Given("An empty task tracker")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])

    When("Adding an app to the taskQueue")
    taskQueue.add(app)

    Then("A new offer matcher gets registered")
    WaitTestSupport.waitUntil("registered as offer matcher", 1.second) {
      offerMatcherManager.offerMatchers.size == 1
    }
    verify(taskTracker).getTasks(app.id)
  }

  test("purging a queue item UNregisters offer matcher") {
    Given("An app in the queue")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    taskQueue.add(app)

    When("The app is purged")
    taskQueue.purge(app.id)

    Then("No offer matchers remain registered")
    assert(offerMatcherManager.offerMatchers.isEmpty)
    verify(taskTracker).getTasks(app.id)
  }

  test("an offer gets unsuccessfully matched against an item in the queue") {
    val offer = MarathonTestHelper.makeBasicOffer().build()

    Given("An app in the queue")
    call(taskTracker.getTasks(app.id)).thenReturn(Map.empty[String, MarathonTask].values)
    taskQueue.add(app)
    WaitTestSupport.waitUntil("registered as offer matcher", 1.second) {
      offerMatcherManager.offerMatchers.size == 1
    }

    When("we ask for matching an offer")
    call(taskFactory.newTask(Matchers.any(), Matchers.any(), Matchers.any())).thenReturn(None)
    val matchFuture = offerMatcherManager.offerMatchers.head.matchOffer(clock.now() + 3.seconds, offer)
    val matchedTasks = Await.result(matchFuture, 3.seconds)

    Then("the offer gets passed to the task factory and respects the answer")
    verify(taskFactory).newTask(Matchers.eq(app), Matchers.eq(offer), Matchers.argThat(SameAsSeq(Seq.empty)))
    assert(matchedTasks.offerId == offer.getId)
    assert(matchedTasks.tasks == Seq.empty)

    verify(taskTracker).getTasks(app.id)
  }

  test("an offer gets successfully matched against an item in the queue") {
    val offer = MarathonTestHelper.makeBasicOffer().build()
    val taskId: TaskID = TaskIdUtil.newTaskId(app.id)
    val mesosTask = MarathonTestHelper.makeOneCPUTask("").setTaskId(taskId).build()
    val marathonTask = MarathonTask.newBuilder().setId(taskId.getValue).build()
    val createdTask = CreatedTask(mesosTask, marathonTask)

    Given("An app in the queue")
    call(taskTracker.getTasks(app.id)).thenReturn(Iterable.empty[MarathonTask])
    call(taskFactory.newTask(Matchers.any(), Matchers.any(), Matchers.any())).thenReturn(Some(createdTask))
    taskQueue.add(app)
    WaitTestSupport.waitUntil("registered as offer matcher", 1.second) {
      offerMatcherManager.offerMatchers.size == 1
    }

    When("we ask for matching an offer")
    val matchFuture = offerMatcherManager.offerMatchers.head.matchOffer(clock.now() + 3.seconds, offer)
    val matchedTasks = Await.result(matchFuture, 3.seconds)

    Then("the offer gets passed to the task factory and respects the answer")
    verify(taskFactory).newTask(Matchers.eq(app), Matchers.eq(offer), Matchers.argThat(SameAsSeq(Seq.empty)))
    assert(matchedTasks.offerId == offer.getId)
    assert(matchedTasks.tasks.map(_.taskInfo) == Seq(mesosTask))

    verify(taskTracker).getTasks(app.id)
  }

  private[this] val app = MarathonTestHelper.makeBasicApp().copy(id = PathId("/app"))

  private[this] var shutdownHooks: ShutdownHooks = _
  private[this] var clock: Clock = _
  private[this] var taskBusModule: TaskBusModule = _
  private[this] var offerMatcherManager: DummyOfferMatcherManager = _
  private[this] var appRepository: AppRepository = _
  private[this] var taskTracker: TaskTracker = _
  private[this] var taskFactory: TaskFactory = _
  private[this] var module: LaunchQueueModule = _

  private[this] def taskQueue = module.taskQueue

  before {
    shutdownHooks = ShutdownHooks()
    clock = Clock()
    taskBusModule = new TaskBusModule()

    offerMatcherManager = new DummyOfferMatcherManager()
    taskTracker = mock[TaskTracker]("taskTracker")
    taskFactory = mock[TaskFactory]("taskFactory")
    appRepository = mock[AppRepository]("appRepository")

    val config: LaunchQueueConfig = new LaunchQueueConfig {}
    config.afterInit()
    module = new LaunchQueueModule(
      config,
      AlwaysElectedLeadershipModule(shutdownHooks),
      clock,
      subOfferMatcherManager = offerMatcherManager,
      taskStatusObservables = taskBusModule.taskStatusObservables,
      maybeOfferReviver = None,
      appRepository,
      taskTracker,
      taskFactory
    )
  }

  after {
    verifyNoMoreInteractions(appRepository)
    verifyNoMoreInteractions(taskTracker)
    verifyNoMoreInteractions(taskFactory)

    shutdownHooks.shutdown()
  }
}

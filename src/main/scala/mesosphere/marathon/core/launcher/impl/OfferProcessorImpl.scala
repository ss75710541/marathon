package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.{ OfferProcessor, OfferProcessorConfig, TaskLauncher }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ MatchedTasks, TaskWithSource }
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import mesosphere.marathon.state.Timestamp
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import org.apache.mesos.Protos.{ OfferID, Offer }
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
  * Passes processed offers to the offerMatcher and launches the appropriate tasks.
  */
private[launcher] class OfferProcessorImpl(
    conf: OfferProcessorConfig, clock: Clock,
    metrics: Metrics,
    offerMatcher: OfferMatcher,
    taskLauncher: TaskLauncher,
    taskTracker: TaskTracker) extends OfferProcessor {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val offerMatchingTimeout = conf.offerMatchingTimeout().millis
  private[this] val saveTasksToLaunchTimeout = conf.saveTasksToLaunchTimeout().millis

  private[this] val incomingOffersMeter =
    metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "incomingOffers"))
  private[this] val matchTimeMeter =
    metrics.timer(metrics.name(MetricPrefixes.SERVICE, getClass, "matchTime"))
  private[this] val matchErrorsMeter =
    metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "matchErrors"))
  private[this] val savingTasksTimeMeter =
    metrics.timer(metrics.name(MetricPrefixes.SERVICE, getClass, "savingTasks"))
  private[this] val savingTasksTimeoutMeter =
    metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "savingTasksTimeouts"))
  private[this] val savingTasksErrorMeter =
    metrics.meter(metrics.name(MetricPrefixes.SERVICE, getClass, "savingTasksErrors"))

  override def processOffer(offer: Offer): Future[Unit] = {
    incomingOffersMeter.mark()

    val matchingDeadline = clock.now() + offerMatchingTimeout
    val savingDeadline = matchingDeadline + saveTasksToLaunchTimeout

    val matchFuture: Future[MatchedTasks] = matchTimeMeter.timeFuture {
      offerMatcher.matchOffer(matchingDeadline, offer)
    }

    matchFuture
      .recover {
        case NonFatal(e) =>
          matchErrorsMeter.mark()
          log.error(s"error while matching '${offer.getId.getValue}'", e)
          MatchedTasks(offer.getId, Seq.empty, resendThisOffer = true)
      }.flatMap {
        case MatchedTasks(offerId, tasks, resendThisOffer) =>
          savingTasksTimeMeter.timeFuture {
            saveTasks(tasks, savingDeadline).map { savedTasks =>
              def notAllSaved: Boolean = savedTasks.size != tasks.size
              MatchedTasks(offerId, savedTasks, resendThisOffer || notAllSaved)
            }
          }
      }.flatMap {
        case MatchedTasks(offerId, Nil, resendThisOffer) => declineOffer(offerId, resendThisOffer)
        case MatchedTasks(offerId, tasks, _)             => launchTasks(offerId, tasks)
      }
  }

  private[this] def declineOffer(offerId: OfferID, resendThisOffer: Boolean): Future[Unit] = {
    //if the offer should be resent, than we ignore the configured decline offer duration
    val duration: Option[Long] = if (resendThisOffer) None else conf.declineOfferDuration.get
    taskLauncher.declineOffer(offerId, duration)
    Future.successful(())
  }

  private[this] def launchTasks(offerId: OfferID, tasks: Seq[TaskWithSource]): Future[Unit] = {
    if (taskLauncher.launchTasks(offerId, tasks.map(_.taskInfo))) {
      log.debug("Offer [{}]. Task launch successful", offerId.getValue)
      tasks.foreach(_.accept())
      Future.successful(())
    }
    else {
      log.warn("Offer [{}]. Task launch rejected", offerId.getValue)
      tasks.foreach(_.reject("driver unavailable"))
      removeTasks(tasks)
    }
  }

  private[this] def removeTasks(tasks: Seq[TaskWithSource]): Future[Unit] = {
    tasks.foldLeft(Future.successful(())) { (terminatedFuture, nextTask) =>
      terminatedFuture.flatMap { _ =>
        val appId = TaskIdUtil.appId(nextTask.marathonTask.getId)
        taskTracker.terminated(appId, nextTask.marathonTask.getId).map(_ => ())
      }
    }.recover {
      case NonFatal(e) =>
        log.error("while removing rejected tasks from taskTracker", e)
        throw e
    }
  }

  /**
    * Saves the given tasks sequentially, evaluating before each save whether the given deadline has been reached
    * already.
    */
  private[this] def saveTasks(tasks: Seq[TaskWithSource], savingDeadline: Timestamp): Future[Seq[TaskWithSource]] = {
    def saveTask(task: TaskWithSource): Future[Option[TaskWithSource]] = {
      log.info("Save task [{}]", task.marathonTask.getId)
      val taskId = task.marathonTask.getId
      val appId = TaskIdUtil.appId(taskId)
      taskTracker.created(appId, task.marathonTask)
      taskTracker
        .store(appId, task.marathonTask)
        .map(_ => Some(task))
        .recoverWith {
          case NonFatal(e) =>
            savingTasksErrorMeter.mark()
            task.reject(s"storage error: $e")
            log.warn(s"error while storing task $taskId for app [$appId]", e)
            taskTracker.terminated(appId, taskId).map(_ => None)
        }.map {
          case Some(savedTask) => Some(task)
          case None            => None
        }
    }

    tasks.foldLeft(Future.successful(Vector.empty[TaskWithSource])) { (savedTasksFuture, nextTask) =>
      savedTasksFuture.flatMap { savedTasks =>
        if (clock.now() > savingDeadline) {
          savingTasksTimeoutMeter.mark(savedTasks.size.toLong)
          nextTask.reject("saving timeout reached")
          log.info(
            s"Timeout reached, skipping launch and save for task ${nextTask.marathonTask.getId}. " +
              s"You can reconfigure this with --${conf.saveTasksToLaunchTimeout.name}.")
          Future.successful(savedTasks)
        }
        else {
          val saveTaskFuture = saveTask(nextTask)
          saveTaskFuture.map(task => savedTasks ++ task)
        }
      }
    }
  }

}

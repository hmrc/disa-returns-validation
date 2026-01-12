package uk.gov.hmrc.disareturnsvalidation.scheduler

import org.apache.pekko.actor.ActorSystem
import play.api.{Configuration, Logging}
import play.api.inject.ApplicationLifecycle
import uk.gov.hmrc.disareturnsvalidation.scheduler.jobs.TransferToNpsJob

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*
import java.util.concurrent.atomic.AtomicBoolean

@Singleton
class Scheduler @Inject()(
                           actorSystem: ActorSystem,
                           lifecycle: ApplicationLifecycle,
                           config: Configuration,
                           transferJob: TransferToNpsJob
                                    )(implicit ec: ExecutionContext) extends Logging {

  private val enabled: Boolean =
    config.getOptional[Boolean]("transfer.scheduler.enabled").getOrElse(true)

  private val initialDelay: FiniteDuration =
    config.getOptional[FiniteDuration]("transfer.scheduler.initialDelay").getOrElse(5.seconds)

  private val interval: FiniteDuration =
    config.getOptional[FiniteDuration]("transfer.scheduler.interval").getOrElse(10.seconds)

  private val running = new AtomicBoolean(false)

  private val cancellable =
    if (enabled) {
      logger.info(s"Starting TransferToNpsJob scheduler: initialDelay=$initialDelay interval=$interval")
      Some(actorSystem.scheduler.scheduleAtFixedRate(initialDelay, interval) { () =>
        run()
      })
    } else {
      logger.warn("TransferToNpsJob scheduler is disabled (transfer.scheduler.enabled=false)")
      None
    }

  lifecycle.addStopHook { () =>
    cancellable.foreach(_.cancel())
    Future.unit
  }

  private def run(): Unit =
    if (running.compareAndSet(false, true)) {
      transferJob.runOnce()
        .recover { case e =>
          logger.error("TransferToNpsJob runOnce failed", e)
        }
        .andThen { case _ =>
          running.set(false)
        }
      ()
    } else {
      logger.debug("TransferToNpsJob run skipped (previous run still in progress)")
    }
}
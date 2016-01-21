package io.travisbrown.concurrent

import cats.data.Xor
import java.util.concurrent.{
  ExecutorService,
  Executors,
  ScheduledExecutorService,
  ThreadFactory,
  TimeUnit,
  TimeoutException
}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

private[concurrent] abstract class GenericContext[F[_]] {
  def apply[A](a: => A): F[A]
  def schedule[A](a: => A, delay: Duration): F[A]
  def fork[A](fa: => F[A]): F[A]
  def delayed[A](fa: F[A], delay: Duration): F[A]
  def withTimeout[A](fa: F[A], timeout: Duration): F[A]

  private[concurrent] def retryInternal[A](
    fa: F[A],
    delays: Stream[Duration],
    accumulating: Boolean
  )(p: Throwable => Boolean): F[(A, List[Throwable])]

  final def retry[A](fa: F[A], delays: Stream[Duration]): F[A] = retryOnly(fa, delays) {
    case NonFatal(error) => true
    case _ => false
  }

  final def retryAccumulating[A](fa: F[A], delays: Stream[Duration]): F[(A, List[Throwable])] =
    retryOnlyAccumulating(fa, delays) {
      case NonFatal(error) => true
      case _ => false
    }

  def retryOnly[A](fa: F[A], delays: Stream[Duration])(p: Throwable => Boolean): F[A]

  final def retryOnlyAccumulating[A](
    fa: F[A],
    delays: Stream[Duration]
  )(p: Throwable => Boolean): F[(A, List[Throwable])] = retryInternal(fa, delays, true)(p)
}

abstract class FutureContext extends GenericContext[Future] {
  final def fork[A](fa: => Future[A]): Future[A] = Future.futureInstance.flatten(apply(fa))
  final def delayed[A](fa: Future[A], delay: Duration): Future[A] = schedule((), delay).flatMap(_ => fa)

  final def retryOnly[A](fa: Future[A], delays: Stream[Duration])(p: Throwable => Boolean): Future[A] =
    retryInternal(fa, delays, false)(p).map(_._1)
}

abstract class Context extends GenericContext[Task] {
  def future: FutureContext

  final def fork[A](fa: => Task[A]): Task[A] = Task.taskInstance.flatten(apply(fa))
  final def delayed[A](fa: Task[A], delay: Duration): Task[A] = new Task(future.delayed(fa.get, delay))

  final def retryOnly[A](fa: Task[A], delays: Stream[Duration])(p: Throwable => Boolean): Task[A] =
    retryInternal(fa, delays, false)(p).map(_._1)
}

final object Context {
  private[this] final val defaultThreadFactory = Executors.defaultThreadFactory()

  /**
   * Default thread factory to mark all threads as daemon
   */
  private[this] val defaultDaemonThreadFactory: ThreadFactory = new ThreadFactory {
    final def newThread(r: Runnable): Thread = {
      val t = defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t
    }
  }

  /**
   * The default executor service is a fixed thread pool with N daemon threads,
   * where N is equal to the number of available processors.
   */
  private[this] val defaultExecutorService: ExecutorService =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors, defaultDaemonThreadFactory)

  /**
   * Default scheduler used for scheduling the tasks like timeout.
   */
  private[this] val defaultScheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1, defaultDaemonThreadFactory)

  val default: Context = new ExecutorContext(defaultExecutorService, defaultScheduler)

  def fromExecutorService(executor: ExecutorService): Context = new ExecutorContext(executor, defaultScheduler)

  private[this] final class ExecutorContext(
    executor: ExecutorService,
    scheduler: ScheduledExecutorService
  ) extends Context {
    val future: FutureContext = new FutureContext {
      final def apply[A](a: => A): Future[A] = new Async[A] {
        final def onFinish(cb: RunnableResultCallback[A]): Unit = {
          executor.submit(
            new Runnable {
              final def run(): Unit = cb.onValue(a).run()
            }
          )
          ()
        }
      }

      final def schedule[A](a: => A, delay: Duration): Future[A] = new Async[A] {
        final def onFinish(cb: RunnableResultCallback[A]): Unit = {
          scheduler.schedule(
            new Runnable {
              def run(): Unit = cb.onValue(a).run()
            },
            delay.toMillis,
            TimeUnit.MILLISECONDS
          )
          ()
        }
      }

      /**
       * Returns a `Future` which returns a `TimeoutException` after `timeoutInMillis`,
       * and attempts to cancel the running computation.
       * This implementation will not block the future's execution thread
       *
       * instead of run this though chooseAny, it is run through simple primitive,
       * as we are never interested in results of timeout callback, and this is more resource savvy
       */
      final def withTimeout[A](fa: Future[A], timeout: Duration): Future[A] = Future.async[A] { callback =>
        val cancel = new AtomicBoolean(false)
        val done = new AtomicBoolean(false)

        scheduler.schedule(
          new Runnable {
            final def run(): Unit = if (done.compareAndSet(false, true)) {
              cancel.set(true)
              callback.onError(new TimeoutException(s"Timed out after ${ timeout.toMillis } milliseconds"))
            } else ()
          },
          timeout.toMillis,
          TimeUnit.MILLISECONDS
        )

        fa.unsafePerformAsyncInterruptibly(
          new ResultCallback[A] {
            final def onValue(value: A): Unit =
              if (done.compareAndSet(false, true)) callback.onValue(value) else ()
            final def onError(error: Throwable): Unit =
              if (done.compareAndSet(false, true)) callback.onError(error) else ()
          },
         cancel
        )
      }

      private[this] final def retryInternalHelper[A](
        fa: Future[A],
        delays: Stream[Duration],
        accumulating: Boolean,
        p: Throwable => Boolean,
        errors: List[Throwable]
      ): Future[(A, List[Throwable])] = delays match {
        case Stream.Empty => fa.map(a => (a, if (accumulating) errors else Nil))
        case head #:: tail => fa.attempt.flatMap {
          case Xor.Right(value) => Future.now((value, if (accumulating) errors else Nil))
          case Xor.Left(error) if p(error) => delayed(
            retryInternalHelper(fa, tail, accumulating, p, error :: errors),
            head
          )
        }
      }

      private[concurrent] final def retryInternal[A](
        fa: Future[A],
        delays: Stream[Duration],
        accumulating: Boolean
      )(p: Throwable => Boolean): Future[(A, List[Throwable])] = retryInternalHelper(fa, delays, accumulating, p, Nil)
    }

    def apply[A](a: => A): Task[A] = new Task(
      new Async[A] {
        final def onFinish(callback: RunnableResultCallback[A]): Unit = {
          executor.submit(
            new Runnable {
              final def run(): Unit = callback.onXor(Xor.catchNonFatal(a)).run()
            }
          )
          ()
        }
      }
    )

    def schedule[A](a: => A, delay: Duration): Task[A] = new Task(
      new Async[A] {
        final def onFinish(cb: RunnableResultCallback[A]): Unit = {
          scheduler.schedule(
            new Runnable {
              def run(): Unit = cb.onXor(Xor.catchNonFatal(a)).run()
            },
            delay.toMillis,
            TimeUnit.MILLISECONDS
          )
          ()
        }
      }
    )

    def withTimeout[A](fa: Task[A], timeout: Duration): Task[A] = new Task(future.withTimeout(fa.get, timeout))

    private[concurrent] def retryInternal[A](
      fa: Task[A],
      delays: Stream[Duration],
      accumulating: Boolean
    )(p: Throwable => Boolean): Task[(A, List[Throwable])] = new Task(
      future.retryInternal(fa.get, delays, accumulating)(p)
    )
  }
}

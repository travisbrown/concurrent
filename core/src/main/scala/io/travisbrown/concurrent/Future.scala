package io.travisbrown.concurrent

import cats.{ Applicative, Eval, MonoidK }
import cats.data.Xor
import java.util.concurrent.{ Callable, ConcurrentLinkedQueue, CountDownLatch }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicBoolean, AtomicReference }
import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
 * `Future` is a trampolined computation producing an `A` that may
 * include asynchronous steps. Like `Trampoline`, arbitrary
 * monadic expressions involving `map` and `flatMap` are guaranteed
 * to use constant stack space. But in addition, one may construct a
 * `Future` from an asynchronous computation, represented as a
 * function, `listen: (A => Unit) => Unit`, which registers a callback
 * that will be invoked when the result becomes available. This makes
 * `Future` useful as a concurrency primitive and as a control
 * structure for wrapping callback-based APIs with a more
 * straightforward, monadic API.
 *
 * Unlike the `Future` implementation in scala 2.10, `map` and
 * `flatMap` do NOT spawn new tasks and do not require an implicit
 * `ExecutionContext`. Instead, `map` and `flatMap` merely add to
 * the current (trampolined) continuation that will be run by the
 * 'current' thread, unless explicitly forked via `Future.fork` or
 * `Future.apply`. This means that `Future` achieves much better thread
 * reuse than the 2.10 implementation and avoids needless thread
 * pool submit cycles.
 *
 * `Future` also differs from the scala 2.10 `Future` type in that it
 * does not necessarily represent a _running_ computation. Instead, we
 * reintroduce nondeterminism _explicitly_ using the functions of the
 * [[Nondeterminism]] interface. This simplifies our implementation
 * and makes code easier to reason about, since the order of effects
 * and the points of nondeterminism are made fully explicit and do not
 * depend on Scala's evaluation order.
 *
 * IMPORTANT NOTE: `Future` does not include any error handling and
 * should generally only be used as a building block by library
 * writers who want to build on `Future`'s capabilities but wish to
 * design their own error handling strategy. See
 * [[Task]] for a type that extends `Future` with
 * proper error handling -- it is merely a wrapper for
 * `Future[Xor[Throwable, A]]` with a number of additional
 * convenience functions.
 */
sealed abstract class Future[A] {
  def flatMap[B](f: A => Future[B]): Future[B]

  def handleErrorWith(f: Throwable => Future[A]): Future[A]

  def run: Xor[Throwable, A]

  def attempt: Future[Xor[Throwable, A]]

  /** Run this `Future` and block awaiting its result. */
  def unsafePerformSync: A

  /**
   * Run this `Future`, passing the result to the given callback once available.
   * Any pure, non-asynchronous computation at the head of this `Future` will
   * be forced in the calling thread. At the first `Async` encountered, control
   * switches to whatever thread backs the `Async` and this function returns.
   */
  def unsafePerformAsync(cb: ResultCallback[A]): Unit

  /**
   * Run this computation to obtain an `A`, then invoke the given callback.
   * Also see `unsafePerformAsync`.
   */
  def listen(cb: RunnableResultCallback[A]): Unit

  /**
   * Run this computation to obtain an `A`, so long as `cancel` remains false.
   * Because of trampolining, we get frequent opportunities to cancel
   * while stepping through the trampoline, so this should provide a fairly
   * robust means of cancellation.
   */
  def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit

  final def map[B](f: A => B): Future[B] = flatMap(a => new Now(f(a)))

  /**
   * Begins running this `Future` and returns a new future that blocks
   * waiting for the result. Note that this will start executing side effects
   * immediately, and is thus morally equivalent to `unsafePerformIO`. The
   * resulting `Future` cannot be rerun to repeat the effects.
   *
   * Use with care.
   */
  final def unsafeStart: Future[A] = new Suspend[A] {
    val latch = new CountDownLatch(1)
    @volatile var result: A = _
    @volatile var bad: Throwable = null
    unsafePerformAsync(
      new ResultCallback[A] {
        final def onValue(a: A): Unit = {
          result = a
          latch.countDown
        }
        final def onError(error: Throwable): Unit = {
          bad = error
          latch.countDown
        }
      }
    )

    def call(): Future[A] = {
      latch.await

      if (bad == null) new Now(result) else new Failed[A](bad)
    }
  }

  /**
   * Run this computation to obtain an `A`, so long as `cancel` remains false.
   * Because of trampolining, we get frequent opportunities to cancel
   * while stepping through the trampoline, this should provide a fairly
   * robust means of cancellation.
   */
  final def unsafePerformAsyncInterruptibly(cb: ResultCallback[A], cancel: AtomicBoolean): Unit =
    listenInterruptibly(cb.toRunnableResultCallback, cancel)

  /**
   * Run this `Future` and block until its result is available, or until
   * `timeoutInMillis` milliseconds have elapsed, at which point a `TimeoutException`
   * will be thrown and the `Future` will attempt to be canceled.
   */
  final def unsafePerformSyncFor(timeoutInMillis: Long): A = unsafePerformSyncAttemptFor(timeoutInMillis) match {
    case Xor.Left(e) => throw e
    case Xor.Right(a) => a
  }

  final def unsafePerformSyncFor(timeout: Duration): A = unsafePerformSyncFor(timeout.toMillis)

  /** Like `unsafePerformSyncFor`, but returns `TimeoutException` as left value.
    * Will not report any other exceptions that may be raised during computation of `A`*/
  final def unsafePerformSyncAttemptFor(timeoutInMillis: Long): Xor[Throwable, A] =
    new InterruptibleRunner[A].runFor(this, timeoutInMillis)

  final def unsafePerformSyncAttemptFor(timeout: Duration): Xor[Throwable, A] =
    unsafePerformSyncAttemptFor(timeout.toMillis)
}

final object Future {
  private[concurrent] val emptyRunnable: Runnable = new Runnable {
    def run(): Unit = ()
  }

  // NB: considered implementing Traverse and Comonad, but these would have
  // to run the Future; leaving out for now

  implicit val futureInstance: Nondeterminism[Future] = new Nondeterminism[Future] {
    override def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)
    def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)

    def pure[A](a: A): Future[A] = now(a)
    override def pureEval[A](a: Eval[A]): Future[A] = new Suspend[A] {
      def call(): Future[A] = now(a.value)
    }

    def chooseAnyIndexed[A](fs: IndexedSeq[Indexed[Future[A]]]): Future[(Indexed[A], IndexedSeq[Indexed[Future[A]]])] =
      new Async[(Indexed[A], IndexedSeq[Indexed[Future[A]]])] {
        final def onFinish(cb: RunnableResultCallback[(Indexed[A], IndexedSeq[Indexed[Future[A]]])]): Unit = {
          // The details of this implementation are a bit tricky, but the general
          // idea is to run all futures in parallel, returning whichever result
          // becomes available first.

          // To account for the fact that the losing computations are still
          // running, we construct special 'residual' Futures for the losers
          // that will first return from the already running computation,
          // then revert back to running the original Future.
          val won = new AtomicBoolean(false) // threads race to set this

          val size = fs.size
          val refs = new Array[AtomicReference[A]](size)
          val listeners = new Array[AtomicReference[RunnableResultCallback[A]]](size)
          val residuals = new Array[Indexed[Future[A]]](size)

          var i = 0

          while (i < size) {
            val f = fs(i)
            val ref = new AtomicReference[A]
            refs(i) = ref
            val listener = new AtomicReference[RunnableResultCallback[A]](null)
            listeners(i) = listener

            val used = new AtomicBoolean(false)

            residuals(i) = new Indexed(
              new Async[A] {
                final def onFinish(innerCb: RunnableResultCallback[A]): Unit =
                  if (used.compareAndSet(false, true)) { // get residual value from already running Future
                    // we've successfully registered ourself with running task
                    if (listener.compareAndSet(null, innerCb)) ()
                    else innerCb.onValue(ref.get).run() // the running task has completed, use its result
                  }
                  else // residual value used up, revert to original Future
                    f.a.listen(innerCb)
              },
              f.i
            )

            i += 1
          }

          i = 0

          while (i < size) {
            val f = fs(i)
            val listener = listeners(i)
            val ref = refs(i)

            f.a.listen(
              new RunnableResultCallback[A] {
                final def onValue(a: A): Runnable = {
                  ref.set(a)
                  val notifyWinner =
                    // If we're the first to finish, invoke `cb`, passing residuals
                    if (won.compareAndSet(false, true)) {
                      System.arraycopy(residuals, i + 1, residuals, i, size - 1 - i)
                      cb.onValue((new Indexed(a, f.i), residuals))
                    } else Future.emptyRunnable // noop; another thread will have already invoked `cb` w/ our residual

                  val notifyListener =
                    if (listener.compareAndSet(null, finishedCallback))
                      // noop; no listeners yet, any added after this will use result stored in `ref`
                      Future.emptyRunnable
                    else // there is a registered listener, invoke it with the result
                      listener.get.onValue(a)

                  new Runnable {
                    def run(): Unit = {
                      notifyWinner.run()
                      notifyListener.run()
                    }
                  }
                }
                final def onError(error: Throwable): Runnable = {
                  val notifyWinner =
                    // If we're the first to finish, invoke `cb`, passing residuals
                    if (won.compareAndSet(false, true)) {
                      cb.onError(error)
                    } else Future.emptyRunnable // noop; another thread will have already invoked `cb` w/ our residual

                  val notifyListener =
                    if (listener.compareAndSet(null, finishedCallback))
                      // noop; no listeners yet, any added after this will use result stored in `ref`
                      Future.emptyRunnable
                    else // there is a registered listener, invoke it with the result
                      listener.get.onError(error)

                  new Runnable {
                    def run(): Unit = {
                      notifyWinner.run()
                      notifyListener.run()
                    }
                  }
                }
              }
            )

            i += 1
          }
        }
      }

    def chooseAny[A](h: Future[A], t: Seq[Future[A]]): Future[(A, Seq[Future[A]])] =
      new Async[(A, Seq[Future[A]])] {
        final def onFinish(cb: RunnableResultCallback[(A, Seq[Future[A]])]): Unit = {
          // The details of this implementation are a bit tricky, but the general
          // idea is to run all futures in parallel, returning whichever result
          // becomes available first.

          // To account for the fact that the losing computations are still
          // running, we construct special 'residual' Futures for the losers
          // that will first return from the already running computation,
          // then revert back to running the original Future.
          val won = new AtomicBoolean(false) // threads race to set this

          val fs = (h +: t).view.zipWithIndex.map { case (f, ind) =>
            val used = new AtomicBoolean(false)
            val ref = new AtomicReference[A]
            val listener = new AtomicReference[RunnableResultCallback[A]](null)
            val residual = new Async[A] {
              final def onFinish(cb: RunnableResultCallback[A]): Unit =
               if (used.compareAndSet(false, true)) { // get residual value from already running Future
                 if (listener.compareAndSet(null, cb)) () // we've successfully registered ourself with running task
                 else cb.onValue(ref.get).run() // the running task has completed, use its result
               }
               else // residual value used up, revert to original Future
                 f.listen(cb)
            }
            (ind, f, residual, listener, ref)
          }.toIndexedSeq

          fs.foreach { case (ind, f, residual, listener, ref) =>
            f.listen(
              new RunnableResultCallback[A] {
                final def onValue(value: A): Runnable = {
                  ref.set(value)
                  val notifyWinner =
                    // If we're the first to finish, invoke `cb`, passing residuals
                    if (won.compareAndSet(false, true))
                      cb.onValue((value, fs.collect { case (i, _, rf, _, _) if i != ind => rf }))
                    else Future.emptyRunnable // noop; another thread will have already invoked `cb` w/ our residual

                  val notifyListener =
                    if (listener.compareAndSet(null, finishedCallback))
                      // noop; no listeners yet, any added after this will use result stored in `ref`
                      Future.emptyRunnable
                    else // there is a registered listener, invoke it with the result
                      listener.get.onValue(value)

                  new Runnable {
                    final def run(): Unit = {
                      notifyWinner.run()
                      notifyListener.run()
                    }
                  }
                }
                final def onError(error: Throwable): Runnable = {
                  val notifyWinner =
                    // If we're the first to finish, invoke `cb`, passing residuals
                    if (won.compareAndSet(false, true)) {
                      cb.onError(error)
                    } else Future.emptyRunnable // noop; another thread will have already invoked `cb` w/ our residual

                  val notifyListener =
                    if (listener.compareAndSet(null, finishedCallback))
                      // noop; no listeners yet, any added after this will use result stored in `ref`
                      Future.emptyRunnable
                    else // there is a registered listener, invoke it with the result
                      listener.get.onError(error)

                  new Runnable {
                    def run(): Unit = {
                      notifyWinner.run()
                      notifyListener.run()
                    }
                  }
                }
              }
            )
          }
        }
      }

    private[this] def finishedCallback[A]: RunnableResultCallback[A] = new RunnableResultCallback[A] {
      private[this] val exception: Exception =
        new RuntimeException("impossible, since there can only be one runner of chooseAny")

      final def onValue(a: A): Runnable = throw exception
      final def onError(error: Throwable): Runnable = throw exception
    }

    // implementation runs all threads, dumping to a shared queue
    // last thread to finish invokes the callback with the results
    override def reduceUnordered[A](fs: Seq[Future[A]]): Future[List[A]] = fs match {
      case Seq() => Future.now(Nil)
      case Seq(f) => f.map(List(_))
      case other => new Async[List[A]] {
        final def onFinish(cb: RunnableResultCallback[List[A]]): Unit = {
          val results = new ConcurrentLinkedQueue[List[A]]
          val c = new AtomicInteger(fs.size)

          fs.foreach { f =>
            f.listen(
              new RunnableResultCallback[A] {
                final def onValue(value: A): Runnable = {
                  // Try to reduce number of values in the queue
                  val front = results.poll()
                  if (front == null) results.add(List(value)) else results.add(value :: front)

                  // only last completed f will hit the 0 here.
                  if (c.decrementAndGet() == 0)
                    cb.onValue(results.asScala.foldLeft[List[A]](Nil)(_ ::: _))
                  else Future.emptyRunnable
                }
                final def onError(error: Throwable): Runnable =
                  if (c.decrementAndGet() == 0) cb.onError(error) else Future.emptyRunnable
              }
            )
          }
        }
      }
    }

    override def reduceUnorderedIndexed[A](fs: IndexedSeq[Indexed[Future[A]]]): Future[List[Indexed[A]]] = fs match {
      case Seq() => Future.now(Nil)
      case Seq(f) => f.a.map(a => List(new Indexed(a, f.i)))
      case other => new Async[List[Indexed[A]]] {
        final def onFinish(cb: RunnableResultCallback[List[Indexed[A]]]): Unit = {
          val results = new ConcurrentLinkedQueue[List[Indexed[A]]]
          val c = new AtomicInteger(fs.size)

          fs.foreach { f =>
            f.a.listen(
              new RunnableResultCallback[A] {
                final def onValue(value: A): Runnable = {
                  // Try to reduce number of values in the queue
                  val front = results.poll()
                  if (front == null)
                    results.add(List(new Indexed(value, f.i)))
                  else results.add(new Indexed(value, f.i) :: front)

                  // only last completed f will hit the 0 here.
                  if (c.decrementAndGet() == 0)
                    cb.onValue(results.asScala.foldLeft[List[Indexed[A]]](Nil)(_ ::: _))
                  else Future.emptyRunnable
                }
                final def onError(error: Throwable): Runnable =
                  if (c.decrementAndGet() == 0) cb.onError(error) else Future.emptyRunnable
              }
            )
          }
        }
      }
    }
  }

  /** type for Futures which need to be executed in parallel when using an Applicative instance */
  type ParallelFuture[A] = Parallel[Future, A]

  /**
   * This Applicative instance runs Futures in parallel.
   *
   * It is different from the Applicative instance obtained from Monad[Future] which runs futures sequentially.
   */
  implicit val futureParallelApplicativeInstance: Applicative[ParallelFuture] = futureInstance.parallel

  /** Convert a strict value to a `Future`. */
  def now[A](a: A): Future[A] = new Now(a)
  def fail[A](error: Throwable): Future[A] = new Failed(error)

  /**
   * Promote a non-strict value to a `Future`. Note that since `Future` is
   * unmemoized, this will recompute `a` each time it is sequenced into a
   * larger computation. Memoize `a` with a lazy value before calling this
   * function if memoization is desired.
   */
  def delay[A](a: => A): Future[A] = new Suspend[A] {
    def call(): Future[A] = new Now(a)
  }

  /**
   * Produce `f` in the main trampolining loop, `Future.step`, using a fresh
   * call stack. The standard trampolining primitive, useful for avoiding
   * stack overflows.
   */
  def suspend[A](f: => Future[A]): Future[A] = new Suspend[A] {
    def call(): Future[A] = f
  }

  def intoFailed[A](f: Future[Xor[Throwable, A]]): Future[A] = f.flatMap {
    case Xor.Right(value) => Future.now(value)
    case Xor.Left(error) => Future.fail(error)
  }

  /**
   * Create a `Future` from an asynchronous computation, which takes the form
   * of a function with which we can register a callback. This can be used
   * to translate from a callback-based API to a straightforward monadic
   * version. See `Task.async` for a version that allows for asynchronous
   * exceptions.
   */
  def async[A](listener: ResultCallback[A] => Unit): Future[A] = new Async[A] {
    final def onFinish(cb: RunnableResultCallback[A]): Unit = listener(cb.toResultCallback)
  }

  /** Calls `Nondeterminism[Future].gatherUnordered`.
   * @since 7.0.3
   */
  def gatherUnordered[A](fs: Seq[Future[A]]): Future[List[A]] = futureInstance.gatherUnordered(fs)
  def reduceUnordered[A](fs: Seq[Future[A]]): Future[List[A]] = futureInstance.reduceUnordered(fs)
}

private[concurrent] final class Now[A](a: A) extends Future[A] {
  final def flatMap[B](f: A => Future[B]): Future[B] = new Suspend[B] {
    final def call(): Future[B] = f(a)
  }

  final def run: Xor[Throwable, A] = Xor.right(a)
  final def attempt: Future[Xor[Throwable, A]] = new Now(Xor.right(a))

  final def handleErrorWith(f: Throwable => Future[A]): Future[A] = this

  final def unsafePerformAsync(cb: ResultCallback[A]): Unit = cb.onValue(a)
  final def unsafePerformSync: A = a
  final def listen(cb: RunnableResultCallback[A]): Unit = cb.onValue(a).run()
  final def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit = cb.onValue(a).run()
}

private[concurrent] final class Failed[A](error: Throwable) extends Future[A] {
  final def flatMap[B](f: A => Future[B]): Future[B] = this.asInstanceOf[Future[B]]

  final def run: Xor[Throwable, A] = Xor.left(error)
  final def attempt: Future[Xor[Throwable, A]] = new Now(Xor.left(error))

  final def handleErrorWith(f: Throwable => Future[A]): Future[A] = f(error)

  final def unsafePerformAsync(cb: ResultCallback[A]): Unit = cb.onError(error)
  final def unsafePerformSync: A = throw error
  final def listen(cb: RunnableResultCallback[A]): Unit = cb.onError(error).run()
  final def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit =
    cb.onError(error).run()
}


private[concurrent] abstract class NotNow[A] extends Future[A] { self =>
  final def run: Xor[Throwable, A] = new Runner[A].run(this)

  final def unsafePerformSync: A = run match {
    case Xor.Right(a) => a
    case Xor.Left(error) => throw error
  }

  final def attempt: Future[Xor[Throwable, A]] = new Async[Xor[Throwable, A]] {
    def onFinish(cb: RunnableResultCallback[Xor[Throwable, A]]): Unit =
      self.listen(
        new RunnableResultCallback[A] {
          final def onValue(a: A): Runnable = cb.onValue(Xor.right(a))
          final def onError(error: Throwable): Runnable = cb.onValue(Xor.left(error))
        }
      )
  }

  final def handleErrorWith(f: Throwable => Future[A]): Future[A] = attempt.flatMap {
    case Xor.Right(value) => Future.now(value)
    case Xor.Left(error) => f(error)
  }
}

private[concurrent] abstract class Suspension[A] extends NotNow[A] with Callable[Future[A]] {
  final def unsafePerformAsync(cb: ResultCallback[A]): Unit = step.listen(cb.toRunnableResultCallback)
  final def listen(cb: RunnableResultCallback[A]): Unit = step.listen(cb)
  final def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit = {
    val afterStep = stepInterruptibly(cancel)

    if (!afterStep.isInstanceOf[Suspension[A]] && !cancel.get) afterStep.listenInterruptibly(cb, cancel) else ()
  }

  /**
   * Evaluate this `Future` to a result, or another asynchronous computation.
   * This has the effect of stripping off any 'pure' trampolined computation at
   * the start of this `Future`.
   */
  @annotation.tailrec
  private def step: Future[A] = this.call() match {
    case s: Suspension[A] => s.step
    case other => other
  }

  /** Like `step`, but may be interrupted by setting `cancel` to true. */
  @annotation.tailrec
  private def stepInterruptibly(cancel: AtomicBoolean): Future[A] =
    if (cancel.get) this else this.call() match {
      case s: Suspension[A] => s.stepInterruptibly(cancel)
      case other => other
    }
}

private[concurrent] abstract class Suspend[A] extends Suspension[A] {
  final def flatMap[B](f: A => Future[B]): Future[B] = new BindSuspend[A, B](this) {
    final def apply(a: A): Future[B] = f(a)
  }
}

private[concurrent] abstract class BindSuspend[T, A](thunk: Callable[Future[T]])
  extends Suspension[A] with Function1[T, Future[A]] { self =>
  final def flatMap[B](f: A => Future[B]): Future[B] = new Suspend[B] {
    def call(): Future[B] = new BindSuspend[T, B](thunk) {
      final def apply(t: T): Future[B] = self(t).flatMap(f)
    }
  }

  final def call(): Future[A] = thunk.call().flatMap(this)
}

private[concurrent] abstract class AsynchronousFuture[T, A] extends NotNow[A] {
  def onFinish(cb: RunnableResultCallback[T]): Unit

  final def unsafePerformAsync(cb: ResultCallback[A]): Unit = listen(cb.toRunnableResultCallback)
}

private[concurrent] abstract class Async[A] extends AsynchronousFuture[A, A] { self =>
  final def flatMap[B](f: A => Future[B]): Future[B] = new BindAsync[A, B] {
    final def onFinish(cb: RunnableResultCallback[A]): Unit = self.onFinish(cb)
    final def onValue(value: A): Future[B] = f(value)
    final def onError(error: Throwable): Future[B] = new Failed(error)
  }

  final def listen(cb: RunnableResultCallback[A]): Unit = onFinish(cb)
  final def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit = onFinish(
    cb.cancellable(cancel)
  )
}

private[concurrent] abstract class BindAsync[T, A]
  extends AsynchronousFuture[T, A] with ResultFold[T, Future[A]] { self =>
  final def flatMap[B](f: A => Future[B]): Future[B] = new Suspend[B] {
    final def call(): Future[B] = new BindAsync[T, B] {
      final def onFinish(cb: RunnableResultCallback[T]): Unit = self.onFinish(cb)
      final def onValue(value: T): Future[B] = self.onValue(value).flatMap(f)
      final def onError(error: Throwable): Future[B] = self.onError(error).flatMap(f)
    }
  }

  final def listen(cb: RunnableResultCallback[A]): Unit = onFinish(
    new RunnableResultCallback[T] {
      final def onValue(value: T): Runnable = new Runnable {
        final def run(): Unit = self.onValue(value).listen(cb)
      }
      final def onError(error: Throwable): Runnable = new Runnable {
        final def run(): Unit = self.onError(error).listen(cb)
      }
    }
  )

  final def listenInterruptibly(cb: RunnableResultCallback[A], cancel: AtomicBoolean): Unit = onFinish(
    new RunnableResultCallback[T] {
      final def onValue(value: T): Runnable = if (cancel.get) Future.emptyRunnable else new Runnable {
        final def run(): Unit = self.onValue(value).listenInterruptibly(cb, cancel)
      }
      final def onError(error: Throwable): Runnable = if (cancel.get) Future.emptyRunnable else new Runnable {
        final def run(): Unit = self.onError(error).listenInterruptibly(cb, cancel)
      }
    }
  )
}

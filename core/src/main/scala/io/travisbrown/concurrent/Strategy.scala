package io.travisbrown.concurrent

import java.util.concurrent.{
  Callable,
  ExecutorService,
  Executors,
  FutureTask,
  ScheduledExecutorService,
  ThreadFactory
}
import javax.swing.{ SwingUtilities, SwingWorker }

/**
 * Evaluate an expression in some specific manner. A typical strategy will schedule asynchronous
 * evaluation and return a function that, when called, will block until the result is ready.
 *
 * Memory consistency effects: Actions in a thread prior to the submission of `a`
 * to the `Strategy` happen-before any actions taken by `a`, which in turn happen-before
 * the result is retrieved via returned function.
 */
abstract class Strategy {
  def call[A](callable: Callable[A]): Callable[A]
  def run(runnable: Runnable): Runnable
}

final object Strategy {
  /**
   * Default thread factory to mark all threads as daemon
   */
  private[this] val defaultDaemonThreadFactory: ThreadFactory = new ThreadFactory {
    val defaultThreadFactory = Executors.defaultThreadFactory()
    def newThread(r: Runnable): Thread = {
      val t = defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t
    }
  }

  /**
   * The default executor service is a fixed thread pool with N daemon threads,
   * where N is equal to the number of available processors.
   */
  val DefaultExecutorService: ExecutorService =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors, defaultDaemonThreadFactory)

  /**
   * Default scheduler used for scheduling the tasks like timeout.
   */
  val DefaultTimeoutScheduler: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1, defaultDaemonThreadFactory)

  /**
   * A strategy that executes its arguments on `DefaultExecutorService`
   */
  implicit val DefaultStrategy: Strategy = Executor(DefaultExecutorService)

  /**
   * A strategy that evaluates its argument in the current thread.
   */
  val Sequential: Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = new Callable[A] {
      val call: A = callable.call()
    }

    def run(runnable: Runnable): Runnable = new Runnable {
      val run: Unit = runnable.run()
    }
  }

  /**
   * A strategy that evaluates its arguments using an implicit ExecutorService.
   */
  def Executor(implicit es: ExecutorService): Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = new Callable[A] {
      private[this] val future = es.submit(callable)
      def call(): A = future.get
    }

    def run(runnable: Runnable): Runnable = new Runnable {
      private[this] val future = es.submit(runnable)
      def run(): Unit = {
        future.get
        ()
      }
    }
  }

  /**
   * A strategy that performs no evaluation of its argument.
   */
  val Id: Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = callable
    def run(runnable: Runnable): Runnable = runnable
  }

  /**
   * A simple strategy that spawns a new thread for every evaluation.
   */
  val Naive: Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = new Callable[A] {
      private[this] val thread = Executors.newSingleThreadExecutor
      private[this] val future = thread.submit(callable)
      thread.shutdown()
      def call(): A = future.get
    }

    def run(runnable: Runnable): Runnable = new Runnable {
      private[this] val thread = Executors.newSingleThreadExecutor
      private[this] val future = thread.submit(runnable)
      thread.shutdown()
      def run(): Unit = {
        future.get
        ()
      }
    }
  }

  /**
   * A strategy that evaluates its arguments using the pool of Swing worker threads.
   */
  val SwingWorker: Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = new Callable[A] {
      private[this] val worker = new SwingWorker[A, Unit] {
        def doInBackground: A = callable.call()
      }
      worker.execute()
      def call(): A = worker.get
    }

    def run(runnable: Runnable): Runnable = new Runnable {
      private[this] val worker = new SwingWorker[Unit, Unit] {
        def doInBackground: Unit = runnable.run()
      }
      worker.execute()
      def run(): Unit = {
        worker.get
        ()
      }
    }
  }

  /**
   * A strategy that evaluates its arguments on the Swing Event Dispatching thread.
   */
  val SwingInvokeLater: Strategy = new Strategy {
    def call[A](callable: Callable[A]): Callable[A] = new Callable[A] {
      private[this] val task = new FutureTask[A](callable)
      SwingUtilities.invokeLater(task)
      def call(): A = task.get
    }

    def run(runnable: Runnable): Runnable = new Runnable {
      private[this] val task = new FutureTask(runnable, ())
      SwingUtilities.invokeLater(task)
      def run(): Unit = task.get
    }
  }
}

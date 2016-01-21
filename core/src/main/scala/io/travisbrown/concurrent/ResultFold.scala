package io.travisbrown.concurrent

import cats.data.Xor
import java.util.concurrent.{ CountDownLatch, TimeoutException, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

trait ResultFold[A, Z] {
  def onValue(value: A): Z
  def onError(error: Throwable): Z

  final def onXor(either: Xor[Throwable, A]): Z = either match {
    case Xor.Right(value) => onValue(value)
    case Xor.Left(error) => onError(error)
  }
}

abstract class ResultCallback[A] extends ResultFold[A, Unit] { self =>
  final def toRunnableResultCallback: RunnableResultCallback[A] = new RunnableResultCallback[A] {
    final def onValue(value: A): Runnable = new Runnable {
      final def run(): Unit = self.onValue(value)
    }

    final def onError(error: Throwable): Runnable = new Runnable {
      final def run(): Unit = self.onError(error)
    }
  }
}

abstract class RunnableResultCallback[A] extends ResultFold[A, Runnable] { self =>
  final def cancellable(cancel: AtomicBoolean): RunnableResultCallback[A] = new RunnableResultCallback[A] {
    final def onValue(value: A): Runnable = if (cancel.get) Future.emptyRunnable else self.onValue(value)
    final def onError(error: Throwable): Runnable = if (cancel.get) Future.emptyRunnable else self.onError(error)
  }

  final def toResultCallback: ResultCallback[A] = new ResultCallback[A] {
    final def onValue(value: A): Unit = self.onValue(value).run()
    final def onError(error: Throwable): Unit = self.onError(error).run()
  }
}

final class Runner[A] extends ResultCallback[A] {
  private[this] val latch = new CountDownLatch(1)
  @volatile private[this] var result: Xor[Throwable, A] = _

  final def onValue(value: A): Unit = {
    result = Xor.right(value)
    latch.countDown
  }

  final def onError(error: Throwable): Unit = {
    result = Xor.left(error)
    latch.countDown
  }

  final def run(f: Future[A]): Xor[Throwable, A] = {
    f.unsafePerformAsync(this)

    latch.await
    result
  }
}

final class InterruptibleRunner[A] extends ResultCallback[A] {
  private[this] val latch = new CountDownLatch(1)
  private[this] val interrupt = new AtomicBoolean(false)
  @volatile private[this] var result: Xor[Throwable, A] = _

  final def onValue(value: A): Unit = {
    result = Xor.right(value)
    latch.countDown
  }

  final def onError(error: Throwable): Unit = {
    interrupt.set(true)
    result = Xor.left(error)
    latch.countDown
  }

  final def runFor(f: Future[A], timeoutInMillis: Long): Xor[Throwable, A] = {
    f.unsafePerformAsyncInterruptibly(this, interrupt)

    if (latch.await(timeoutInMillis, TimeUnit.MILLISECONDS)) result else {
      interrupt.set(true)
      Xor.Left(new TimeoutException(s"Timed out after $timeoutInMillis milliseconds"))
    }
  }
}

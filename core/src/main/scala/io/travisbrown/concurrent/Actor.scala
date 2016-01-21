package io.travisbrown.concurrent

import cats.functor.Contravariant
import java.util.concurrent.atomic.AtomicReference

/**
 * Processes messages of type `A`, one at a time. Messages are submitted to
 * the actor with the method `!`. Processing is typically performed asynchronously,
 * this is controlled by the provided `strategy`.
 *
 * Memory consistency guarantee: when each message is processed by the `handler`, any memory that it
 * mutates is guaranteed to be visible by the `handler` when it processes the next message, even if
 * the `strategy` runs the invocations of `handler` on separate threads. This is achieved because
 * the `Actor` reads a volatile memory location before entering its event loop, and writes to the same
 * location before suspending.
 *
 * Implementation based on non-intrusive MPSC node-based queue, described by Dmitriy Vyukov:
 * [[http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue]]
 *
 * @param handler  The message handler
 * @param onError  Exception handler, called if the message handler throws any `Throwable`.
 * @param strategy Execution strategy, for example, a strategy that is backed by an `ExecutorService`
 * @tparam A       The type of messages accepted by this actor.
 */
case class Actor[A](
  callback: ResultCallback[A]
)(implicit val strategy: Strategy) { self =>
  private[this] val head = new AtomicReference[Node[A]]

  /** Pass the message `a` to the mailbox of this actor */
  final def apply(a: A): Unit = {
    val n = new Node(a)
    val h = head.getAndSet(n)
    if (h ne null) h.lazySet(n) else schedule(n)
  }

  final def contramap[B](f: B => A): Actor[B] = new Actor[B](
    new ResultCallback[B] {
      final def onValue(value: B): Unit = callback.onValue(f(value))
      final def onError(error: Throwable): Unit = callback.onError(error)
    }
  )(strategy)

  private[this] def schedule(n: Node[A]): Unit = strategy.run(
    new Runnable {
      def run(): Unit = act(n)
    }
  )

  @annotation.tailrec
  private[this] def act(n: Node[A], i: Int = 1024): Unit = {
    try callback.onValue(n.a) catch {
      case error: Throwable => callback.onError(error)
    }
    val n2 = n.get
    if (n2 eq null) scheduleLastTry(n)
    else if (i == 0) schedule(n2)
    else act(n2, i - 1)
  }

  private[this] def scheduleLastTry(n: Node[A]): Unit = strategy.run(
    new Runnable {
      def run(): Unit = lastTry(n)
    }
  )

  private[this] def lastTry(n: Node[A]): Unit = if (!head.compareAndSet(n, null)) act(next(n))

  @annotation.tailrec
  private[this] def next(n: Node[A]): Node[A] = {
    val n2 = n.get
    if (n2 ne null) n2
    else next(n)
  }
}

private class Node[A](final val a: A) extends AtomicReference[Node[A]]

final object Actor {
  implicit val actorContravariant: Contravariant[Actor] = new Contravariant[Actor] {
    def contramap[A, B](r: Actor[A])(f: B => A): Actor[B] = r.contramap(f)
  }

  def actor[A](
    valueHandler: A => Unit,
    errorHandler: Throwable => Unit
  )(implicit s: Strategy): Actor[A] = new Actor[A](
    new ResultCallback[A] {
      final def onValue(value: A): Unit = valueHandler(value)
      final def onError(error: Throwable): Unit = errorHandler(error)
    }
  )(s)

  def rethrowing[A](handler: A => Unit)(implicit s: Strategy): Actor[A] = actor(handler, throw _)
}

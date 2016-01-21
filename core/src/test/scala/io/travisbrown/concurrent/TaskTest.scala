package io.travisbrown.concurrent

import algebra.Eq
import cats.{ ApplicativeError, Eval, Monad }
import cats.data.Xor
import cats.laws.discipline.{ MonadErrorTests, MonoidalTests }
import java.util.concurrent.{ Executors, TimeoutException, TimeUnit }
import java.util.concurrent.atomic._
import org.scalacheck.{ Arbitrary, Gen }

class TaskTest extends BaseTest {
  val error = ApplicativeError[Task, Throwable]

  implicit def taskEq[A](implicit A: Eq[A]): Eq[Task[A]] =
    Eq.by((_: Task[A]).attempt.unsafePerformSync)

  implicit def arbitraryTask[A](implicit A: Arbitrary[A]): Arbitrary[Task[A]] = Arbitrary(
    A.arbitrary.flatMap(a => Gen.oneOf(Task.now(a), Task.delay(a)))
  )

  checkAll("TaskInt]", MonadErrorTests[Task, Throwable].monadError[Int, Int, Int])
  checkAll("Task[Int]", MonoidalTests[Task].monoidal[Int, Int, Int])

  val N = 10000
  val correct = (0 to N).sum

  // standard worst case scenario for trampolining -
  // huge series of left associated binds
  def leftAssociatedBinds(seed: (=> Int) => Task[Int], cur: (=> Int) => Task[Int]): Task[Int] =
    (0 to N).map(cur(_)).foldLeft(seed(0))((a, b) => Task.taskInstance.map2(a, b)(_ + _))

  val options = List[(=> Int) => Task[Int]](n => Task.now(n), Task.delay _, Context.default.apply _)
  val combinations = (options |@| options).tupled

  test("left associated binds") {
    assert(
      combinations.forall {
        case (seed, cur) => leftAssociatedBinds(seed, cur).unsafePerformSync === correct
      }
    )
  }

  test("traverse-based map == sequential map") {
    check { (xs: List[Int]) =>
      xs.map(_ + 1) === xs.traverse(x => Context.default(x + 1)).unsafePerformSync
    }
  }

  test("gather-based map == sequential map") {
    check { (xs: List[Int]) =>
      xs.map(_ + 1) === Nondeterminism[Task].gather(xs.map(x => Context.default(x + 1))).unsafePerformSync.toList
    }
  }

  case object FailWhale extends RuntimeException {
    override def fillInStackTrace = this
  }

  case object SadTrombone extends RuntimeException {
    override def fillInStackTrace = this
  }

  case object FailTurkey extends Error {
    override def fillInStackTrace = this
  }

  test("catches exceptions") {
    val task = Context.default { Thread.sleep(10); throw FailWhale; 42 }.map(_ + 1)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches errors") {
    val task = Context.default { Thread.sleep(10); throw FailTurkey; 42 }.map(_ + 1)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailTurkey))
  }

  test("catches exceptions in a mapped function") {
    val task = Context.default { Thread.sleep(10); 42 }.map[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in a mapped function, created by delay") {
    val task = Task.delay { Thread.sleep(10); 42 }.map[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in a mapped function, created with now") {
    val task = Task.now { Thread.sleep(10); 42 }.map[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in a flatMapped function") {
    val task = Context.default { Thread.sleep(10); 42 }.flatMap[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in a flatMapped function, created with delay") {
    val task = Task.delay { Thread.sleep(10); 42 }.flatMap[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in a flatMapped function, created with now") {
    val task = Task.now { Thread.sleep(10); 42 }.flatMap[Int](_ => throw FailWhale)
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions in parallel execution") {
    check { (x: Int, y: Int) =>
      val t1 = Context.default { Thread.sleep(10); throw FailWhale; 42 }
      val t2 = Context.default { 43 }
      Nondeterminism[Task].both(t1, t2).unsafePerformSyncAttempt === Xor.left(FailWhale)
    }
  }

  test("handles exceptions in recover") {
    val task = error.recover(Context.default { Thread.sleep(10); throw FailWhale; 42 }) { case FailWhale => 84 }
    assert(task.unsafePerformSyncAttempt === Xor.right(84))
  }

  test("leaves unhandled exceptions alone in recover") {
    val task = error.recover(Context.default { Thread.sleep(10); throw FailWhale; 42 }){ case SadTrombone => 84 }
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions thrown in recover") {
    val task = error.recover(
      Context.default { Thread.sleep(10); throw FailWhale; 42 }
    ) { case FailWhale => throw SadTrombone }
    assert(task.unsafePerformSyncAttempt === Xor.left(SadTrombone))
  }

  test("handles exceptions in recoverWith") {
    val task = error.recoverWith(
      Context.default { Thread.sleep(10); throw FailWhale; 42 }
    ) { case FailWhale => Task.delay(84) }
    assert(task.unsafePerformSyncAttempt === Xor.right(84))
  }

  test("leaves unhandled exceptions alone in recoverWith") {
    val task = error.recoverWith(
      Context.default { Thread.sleep(10); throw FailWhale; 42 }
    ) { case SadTrombone => Task.delay(84) }
    assert(task.unsafePerformSyncAttempt === Xor.left(FailWhale))
  }

  test("catches exceptions thrown in recoverWith") {
    val task = error.recoverWith(Context.default { Thread.sleep(10); throw FailWhale; 42 }) {
      case FailWhale => Task.delay[Int](throw SadTrombone)
    }

    assert(task.unsafePerformSyncAttempt === Xor.left(SadTrombone))
  }

  test("evaluates Monad[Task].pureEval lazily") {
    val M = implicitly[Monad[Task]]
    var x = 0
    M.pureEval(Eval.later(x += 1))
    assert(x === 0)
  }

  val es = Executors.newFixedThreadPool(1)
  val context = Context.fromExecutorService(es)

  test("Nondeterminism[Task] should correctly process reduceUnordered for >1 tasks in non-blocking way") {
    val t1 = context.fork(Task.now(1))
    val t2 = Task.delay(7).flatMap(_ => context.fork(Task.now(2)))
    val t3 = context.fork(Task.now(3))
    val t = context.fork(Task.reduceUnordered[Int](Seq(t1, t2, t3)))

    assert(t.unsafePerformSync === List(1, 2, 3))
  }

  test("correctly process reduceUnordered for 1 task in non-blocking way") {
    val t1 = context.fork(Task.now(1))
    val t = context.fork(Task.reduceUnordered[Int](Seq(t1)))

    assert(t.unsafePerformSync === List(1))
  }

  test("correctly process reduceUnordered for empty seq of tasks in non-blocking way") {
    val t = context.fork((Task.reduceUnordered[Int](Seq())))

    assert(t.unsafePerformSync === Nil)
  }

  test("early terminate once any of the tasks failed") {
    val ex = new RuntimeException("expected")
    val t1v = new AtomicInteger(0)
    val t3v = new AtomicInteger(0)

    val es3 = Executors.newFixedThreadPool(3)
    val context = Context.fromExecutorService(es3)

    // NB: Task can only be interrupted in between steps (before the `map`)
    val t1 = Context.default.fork { Thread.sleep(1000); Task.Unit }.map { _ => t1v.set(1) }
    val t2 = Context.default.fork { Task.now[Unit](throw ex) }
    val t3 = Context.default.fork { Thread.sleep(1000); Task.Unit }.map { _ => t3v.set(3) }
    val t = context.fork(Task.gatherUnordered(Seq(t1, t2, t3), exceptionCancels = true))

    assert(t.unsafePerformSyncAttempt === Xor.left(ex))
    assert(t1v.get === 0)
    assert(t3v.get === 0)
  }

  test("early terminate once any of the tasks failed, and cancels execution") {
    val ex = new RuntimeException("expected")
    val t1v = new AtomicInteger(0)
    val t3v = new AtomicInteger(0)

    implicit val es3 = Executors.newFixedThreadPool(3)
    val context = Context.fromExecutorService(es3)

    // NB: Task can only be interrupted in between steps (before the `map`)
    val t1 = Context.default.fork { Thread.sleep(1000); Task.Unit }.map { _ => t1v.set(1) }
    val t2 = Context.default.fork { Thread.sleep(100); Task.now[Unit](throw ex) }
    val t3 = Context.default.fork { Thread.sleep(1000); Task.Unit }.map { _ => t3v.set(3) }
    val t = context.fork(Task.gatherUnordered(Seq(t1, t2, t3), exceptionCancels = true))

    assert(t.unsafePerformSyncAttempt === Xor.Left(ex))

    Thread.sleep(3000)

    assert(t1v.get === 0)
    assert(t3v.get === 0)
  }

  test("nmap6 must run Tasks in parallel") {
    import java.util.concurrent.CyclicBarrier

    //Ensure at least 6 different threads are available.
    val es6 = Executors.newFixedThreadPool(6)
    val context = Context.fromExecutorService(es6)
    val barrier = new CyclicBarrier(6)

    val seenThreadNames = scala.collection.JavaConversions.asScalaSet(
      java.util.Collections.synchronizedSet(new java.util.HashSet[String]())
    )

    val t = for (i <- 0 to 5) yield context.fork {
      seenThreadNames += Thread.currentThread().getName()
      //Prevent the execution scheduler from reusing threads. This will only
      //proceed after all 6 threads reached this point.
      barrier.await(1, TimeUnit.SECONDS)
      Task.now(('a' + i).toChar)
    }

    val r = Nondeterminism[Task].nmap6(t(0), t(1), t(2), t(3), t(4), t(5))(List(_, _, _, _, _, _))
    val chars = List('a', 'b', 'c', 'd', 'e', 'f')
    assert(r.unsafePerformSync === chars)
      //Ensure we saw 6 distinct threads.
    assert(seenThreadNames.size === 6)
  }

  test("correctly exit when timeout is exceeded on runFor") {
    val es = Executors.newFixedThreadPool(1)
    val context = Context.fromExecutorService(es)

    val t = context.fork { Thread.sleep(3000); Task.now(1) }

    assert(
      t.unsafePerformSyncAttemptFor(100) match {
        case Xor.Left(_: TimeoutException) => true
        case _ => false
      }
    )

    es.shutdown()
  }

  test("correctly cancels scheduling of all tasks once first task hit timeout") {
    val es = Executors.newFixedThreadPool(1)
    val context = Context.fromExecutorService(es)

    @volatile var bool = false

    val t = context.fork { Thread.sleep(1000); Task.now(1) }.map(_ => bool = true)

    assert(
      t.unsafePerformSyncAttemptFor(100) match {
        case Xor.Left(_: TimeoutException) => true
        case _ => false
      }
    )

    Thread.sleep(1500)

    assert(!bool)

    es.shutdown()
  }

  test("retries a retriable task n times") {
    check { (xs: Stream[Byte]) =>
      import scala.concurrent.duration._
      var x = 0
      val task = Task.delay[Unit] { x += 1; sys.error("oops") }
      Context.default.retry(task, xs.map(_ => 0.milliseconds)).attempt.unsafePerformSync
      x === (xs.length + 1)
    }
  }

  test("fromOption fails on None") {
    check { (t: Throwable) =>
      Task.fromOption[Int](None)(t).unsafePerformSyncAttempt.isLeft
    }
  }

  test("fromOption succeeds on Some") {
    check { (n: Int, t: Throwable) =>
      Task.fromOption[Int](Some(n))(t).unsafePerformSyncAttempt.isRight
    }
  }

  test("fromXor matches attemptRun") {
    check { (x: Either[Throwable, Int]) =>
      Task.fromXor(Xor.fromEither(x)).unsafePerformSyncAttempt === Xor.fromEither(x)
    }
  }
}

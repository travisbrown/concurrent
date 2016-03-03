package io.travisbrown.concurrent

import algebra.Eq
import cats.laws.discipline.{ CartesianTests, MonadTests }
import java.util.concurrent._
import org.scalacheck.{ Arbitrary, Gen }

class FutureTest extends BaseTest {
  implicit def futureEq[A](implicit A: Eq[A]): Eq[Future[A]] =
    Eq.by((_: Future[A]).unsafePerformSync)

  implicit def arbitraryFuture[A](implicit A: Arbitrary[A]): Arbitrary[Future[A]] = Arbitrary(
    A.arbitrary.flatMap(a => Gen.oneOf(Future.now(a), Future.delay(a)))
  )

  checkAll("Future[Int]", MonadTests[Future].monad[Int, Int, Int])
  checkAll("Future[Int]", CartesianTests[Future].cartesian[Int, Int, Int])

  val non = Nondeterminism[Future]

  test("Future should not deadlock when using Nondeterminism#chooseAny") {
    withTimeout(2000) {
      deadlocks(3).unsafePerformSync.length === 4
    }
  }

  test("Future should have a run method that returns when constructed from Future.now") {
    check { (n: Int) =>
      Future.now(n).unsafePerformSync === n
    }
  }

  test("Future should have a run method that returns when constructed from Future.delay") {
    check { (n: Int) =>
      Future.delay(n).unsafePerformSync === n
    }
  }

  test("Future should have an unsafePerformSync method that returns when constructed by forking another future") {
    check { (n: Int) =>
      Context.default.future.fork(Future.now(n)).unsafePerformSync === n
    }
  }

  test("Future should have a run method that returns when constructed from Future.suspend") {
    check { (n: Int) =>
      Future.suspend(Future.now(n)).unsafePerformSync === n
    }
  }

  test("Future should have a run method that returns when constructed from Future.async") {
    check { (n: Int) =>
      def callback(call: ResultCallback[Int]): Unit = call.onValue(n)
      Future.async(callback).unsafePerformSync === n
    }
  }

  test("Future should have an unsafePerformSync method that returns when constructed from a Pool") {
    check { (n: Int) =>
      Context.default.future(n).unsafePerformSync === n
    }
  }

  test("Nondeterminism[Future] should correctly process reduceUnordered for >1 futures in non-blocking way") {
    val es = Executors.newFixedThreadPool(1)
    val context = Context.fromExecutorService(es)

    val f1 = context.future.fork(Future.now(1))
    val f2 = Future.delay(7).flatMap(_ => context.future.fork(Future.now(2)))
    val f3 = context.future.fork(Future.now(3))

    val f = context.future.fork(Future.reduceUnordered[Int](Seq(f1, f2, f3)))

    assert(f.unsafePerformSync.toSet === Set(1, 2, 3))
  }

  test("Nondeterminism[Future] should correctly process reduceUnordered for 1 future in non-blocking way") {
    val es = Executors.newFixedThreadPool(1)
    val context = Context.fromExecutorService(es)

    val f1 = context.future.fork(Future.now(1))
    val f = context.future.fork(Future.reduceUnordered[Int](Seq(f1)))

    assert(f.unsafePerformSync === List(1))
  }


  test("Nondeterminism[Future] should correctly process reduceUnordered for empty seq of futures in non-blocking way") {
    val es = Executors.newFixedThreadPool(1)
    val context = Context.fromExecutorService(es)

    val f = context.future.fork(Future.reduceUnordered[Int](Seq()))

    assert(f.unsafePerformSync === Nil)
  }
  
  test("Timed Future should not run futures sequentially") {
    val times = Stream.iterate(100)(_ + 100).take(10)

    val start = System.currentTimeMillis()
    val result = Context.default.future.fork(
      Future.gatherUnordered(
        times.map { time =>
          Context.default.future.fork {
            Thread.sleep(time.toLong)
            Future.now(time)
          }
        }
      )
    ).unsafePerformSync
    val duration = System.currentTimeMillis() - start

    assert(result.length === times.size && duration.toInt < times.fold(0)(_ + _))
  }

  /*
   * This is a little deadlock factory based on the code in #308.
   *
   * Basically it builds a tree of futures that run in a
   * non-determistic order. The sleep(x) provides an increase
   * in the number of collisions, and chance for deadlock.
   *
   * Before #312 patch, this triggered deadlock approx 1 in every
   * 3 runs.
   */
  def deadlocks(depth: Int): Future[List[Long]] = if (depth == 1) Context.default.future.fork(
    Future.delay { Thread.sleep(20); List(System.currentTimeMillis) }
  ) else Context.default.future.fork(
    non.both(deadlocks(depth - 1), deadlocks(depth - 1)).map {
      case (l, r) => l ++ r
    }
  )
}

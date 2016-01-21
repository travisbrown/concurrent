package io.travisbrown.concurrent

import algebra.Eq
import cats.std.AllInstances
import cats.syntax.AllSyntax
import java.util.concurrent._
import org.scalacheck.Prop
import org.scalatest.{ FunSuite, WordSpec }
import org.scalatest.prop.Checkers
import org.typelevel.discipline.scalatest.Discipline

class BaseTest extends FunSuite with Checkers with Discipline with AllInstances with AllSyntax {
  implicit val eqThrowable: Eq[Throwable] = Eq.fromUniversalEquals

  implicit def eqTuple2[A, B](implicit A: Eq[A], B: Eq[B]): Eq[(A, B)] = Eq.instance {
    case ((a1, a2), (b1, b2)) => A.eqv(a1, b1) && B.eqv(a2, b2)
  }

  implicit def eqTuple3[A, B, C](implicit A: Eq[A], B: Eq[B], C: Eq[C]): Eq[(A, B, C)] = Eq.instance {
    case ((a1, b1, c1), (a2, b2, c2)) => A.eqv(a1, a2) && B.eqv(b1, b2) && C.eqv(c1, c2)
  }

  override def convertToEqualizer[T](left: T): Equalizer[T] =
    sys.error("Intentionally ambiguous implicit for Equalizer")

  def assertCountDown(latch: CountDownLatch, hint: String, timeout: Long = 1000): Prop = {
    if (latch.await(timeout, TimeUnit.MILLISECONDS)) true
    else sys.error("Failed to count down within " + timeout + " millis: " + hint)
  }

  def fork(f: => Unit): Unit =
    new Thread {
      override def run(): Unit = f
    }.start()

  final class WithTimeout(timeout: Long) {
    def apply[A](test: => A): A = {
      val latch = new CountDownLatch(1)
      @volatile var result: A = null.asInstanceOf[A]
      fork {
        result = test
        latch.countDown
      }
      if (latch.await(timeout, TimeUnit.MILLISECONDS)) result else sys.error("Timeout occured, possible deadlock.")
    }
  }

  def withTimeout(timeout: Long): WithTimeout = new WithTimeout(timeout)
}

class ConcurrentTest extends BaseTest {
  test("Current test tools should succeed on exhaused CountDownLatch") {
    val latch = new CountDownLatch(1)
    latch.countDown
    assertCountDown(latch, "Should always be processed immediately")
  }

  test("Current test tools should run forked code") {
    val latch = new CountDownLatch(1)
    fork {
      latch.countDown
    }
    assertCountDown(latch, "Should be processed asynchronously")
  }

  test("Current test tools should run test with timeout") {
    withTimeout(2000) {
      true
    }
  }

  test("Current test tools should fail when timeout occurs") {
    intercept[RuntimeException](
      withTimeout(100) {
        Thread.sleep(2000)
        ()
      }
    )
  }
}

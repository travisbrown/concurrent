package io.travisbrown.concurrent

import collection.mutable
import java.util.concurrent._

class ActorTest extends BaseTest {
  val NumOfMessages = 1000
  val NumOfThreads = 4
  val NumOfMessagesPerThread = NumOfMessages / NumOfThreads
  implicit val executor = Executors.newFixedThreadPool(NumOfThreads)

  test("code executes async") {
    val latch = new CountDownLatch(1)
    val actor = Actor.rethrowing[Int]((i: Int) => latch.countDown())
    actor(1)
    assertCountDown(latch, "Should process a message")
  }

  test("code errors are catched and can be handled") {
    val latch = new CountDownLatch(1)
    val actor = Actor.actor[Int]((i: Int) => 100 / i, (ex: Throwable) => latch.countDown())
    actor(0)
    assertCountDown(latch, "Should catch an exception")
  }

  test("actors exchange messages without loss") {
    val latch = new CountDownLatch(NumOfMessages)
    var actor1: Actor[Int] = null
    val actor2 = Actor.rethrowing[Int]((i: Int) => actor1(i - 1))
    actor1 = Actor.rethrowing[Int] {
      (i: Int) =>
        if (i == latch.getCount) {
          latch.countDown()
          latch.countDown()
          if (i != 0) actor2(i - 1)
        }
    }
    actor1(NumOfMessages)
    assertCountDown(latch, "Should exchange " + NumOfMessages + " messages")
  }

  test("actor handles messages in order of sending by each thread") {
    val latch = new CountDownLatch(NumOfMessages)
    val actor = countingDownActor(latch)
    for (j <- 1 to NumOfThreads) fork {
      for (i <- 1 to NumOfMessagesPerThread) {
        actor((j, i))
      }
    }
    assertCountDown(latch, "Should process " + NumOfMessages + " messages")
  }

  def countingDownActor(latch: CountDownLatch): Actor[(Int, Int)] = Actor.rethrowing[(Int, Int)] {
    val ms = mutable.Map[Int, Int]()

    (m: (Int, Int)) =>
      val (j, i) = m
      if (ms.getOrElse(j, 0) + 1 == i) {
        ms.put(j, i)
        latch.countDown()
      }
  }
}

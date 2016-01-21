package io.travisbrown.concurrent

import cats.data.Xor
import java.util.concurrent.{ThreadFactory, Executors}
import scala.collection.immutable.Queue
import scala.concurrent.SyncVar

/**
 *
 * User: pach
 * Date: 7/9/13
 * Time: 6:53 PM
 * (c) 2011-2013 Spinoco Czech Republic, a.s.
 */
class ConcurrentTaskTest extends BaseTest {
  test("Task should correctly use threads when forked and flatmapped") {
    @volatile var q = Queue[(Int, String)]()

    val forked = "forked-thread"
    val current =  Thread.currentThread().getName

    def enqueue(taskId: Int) =
      q = q.enqueue((taskId, Thread.currentThread().getName))

    val es = Executors.newFixedThreadPool(1, new ThreadFactory {
      def newThread(p1: Runnable) = new Thread(p1, forked)
    })

    val context = Context.fromExecutorService(es)

    val sync = new SyncVar[Boolean]

    (for {
      _ <- Task.now(enqueue(1))
      _ <- Task.delay(enqueue(2))
      _ <- context.fork(Task.now(enqueue(3)))
      _ <- Task.delay(enqueue(4))
      _ <- context.fork(Task.now(enqueue(5)))
      _ <- Task.now(enqueue(6))
      _ <- context.fork(Task.delay(enqueue(7)))
    } yield ()).unsafePerformAsync(_ => {
      enqueue(8)
      sync.put(true)
    })
    enqueue(9)

    assert(sync.get(5000) === Some(true))

    val runned = q.toList
      
    //trampoline should be evaluated at the head before anything else gets evaluated
    assert(runned(0) === ((1, current)))
    assert(runned(1) === ((2, current)))
      
    //the after async must not be the last ever
    assert(runned.last._1 != 9)
      
    //the rest of tasks must be run off the forked thread
    assert(runned.filter(_._2 == forked).map(_._1) === List(3, 4, 5, 6, 7, 8))
  }

  test("complete even when interrupted") {
    val t = Context.default.fork(Task.delay(Thread.sleep(3000)))
    val sync = new SyncVar[Xor[Throwable, Unit]]
    val interrupt = t.unsafePerformAsyncInterruptibly(sync.put)
    Thread.sleep(1000)
    interrupt()
    assert(sync.get(3000) === Some(Xor.left(Task.TaskInterrupted)))
  }
}

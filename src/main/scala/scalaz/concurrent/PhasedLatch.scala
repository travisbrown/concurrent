package scalaz
package concurrent

import effect.IO

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.AbstractQueuedSynchronizer

sealed trait PhasedLatch {
  /** Release the current phase. */
  def release(): IO[Unit]

  /** Await the current phase. */
  @throws(classOf[InterruptedException])
  final def await() = currentPhase flatMap awaitPhase

  /** Await the current phase for the specified period. */
  @throws(classOf[InterruptedException])
  final def awaitFor(time: Long, unit: TimeUnit) = currentPhase flatMap { awaitPhaseFor(_, time, unit) }

  /** Await for the specified phase.*/
  @throws(classOf[InterruptedException])
  def awaitPhase(phase: Int): IO[Unit]

  /** Await the specified phase for the specified period.*/
  @throws(classOf[InterruptedException])
  def awaitPhaseFor(phase: Int, period: Long, unit: TimeUnit): IO[Boolean]

  def currentPhase: IO[Int]
}

object PhasedLatch extends PhasedLatches

trait PhasedLatches {
  import Ordering._
  private[this] lazy val phaseOrder = Order.order[Int] { (a, b) =>
    (a - b) match {
      case 0 => EQ
      case x if x > 0 => GT
      case y if y < 0 => LT
    }
  }

  def newPhasedLatch: IO[PhasedLatch] = IO(new PhasedLatch {
    /** This sync implements Phasing. The state represents the current phase as
     *  an integer that continually increases. The phase can wrap around past
     *  Int#MaxValue
     */
    val sync = new AbstractQueuedSynchronizer {
      def currentPhase = getState
  
      
      override def tryAcquireShared(waitingFor: Int) =
        if (phaseOrder.lessThan(waitingFor, currentPhase)) 1
        else -1
  
      @annotation.tailrec
      override def tryReleaseShared(ignore: Int) = {
        val state = getState
        if (compareAndSetState(state, state + 1)) true
        else tryReleaseShared(ignore)
      }
    }
  
    /** Release the current phase. */
    def release = IO { sync releaseShared 1 }
  
    /** Await for the specified phase.*/
    @throws(classOf[InterruptedException])
    def awaitPhase(phase: Int) = IO { sync acquireSharedInterruptibly phase }
  
    @throws(classOf[InterruptedException])
    def awaitPhaseFor(phase: Int, period: Long, unit: TimeUnit) = IO {
      sync.tryAcquireSharedNanos(phase, unit.toNanos(period))
    }
  
    def currentPhase = IO(sync.currentPhase)
  })
}

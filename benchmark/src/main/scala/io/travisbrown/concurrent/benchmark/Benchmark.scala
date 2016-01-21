package io.travisbrown.concurrent.benchmark

import cats.Traverse
import com.twitter.util.{ Await => AwaitT, Future => FutureT, FuturePool }
import io.travisbrown.concurrent.{ Context, Future, Task }
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future => FutureS }
import scala.concurrent.duration.Duration
import scalaz.{ Traverse => TraverseZ }
import scalaz.std.vector.{ vectorInstance => vectorInstanceZ }
import scalaz.concurrent.{ Future => FutureZ, Task => TaskZ }

/**
 * Compare the performance of iteratee operations.
 *
 * The following command will run the benchmarks with reasonable settings:
 *
 * > sbt "benchmark/jmh:run -i 10 -wi 10 -f 2 -t 1 io.travisbrown.concurrent.benchmark.TraverseBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class TraverseBenchmark {
  val range: Vector[Int] = (100 to 10100).toVector

  implicit val es: java.util.concurrent.ExecutorService = new java.util.concurrent.ForkJoinPool()
  val pool = FuturePool(es)

  def sumOfFactors(n: Int): Int = (1 to n)./*filter(i => n % i == 0).*/sum

  //def sofI(i: Int): Task[Int] = Task(sumOfFactors(i)).flatMap(v => Task(sumOfFactors(v)))
  //def sofZ(i: Int): TaskZ[Int] = TaskZ(sumOfFactors(i)).flatMap(v => TaskZ(sumOfFactors(v)))
  //def sofS(i: Int): FutureS[Int] = FutureS(sumOfFactors(i)).flatMap(v => FutureS(sumOfFactors(v)))
  //def sofT(i: Int): FutureT[Int] = pool(sumOfFactors(i)).flatMap(v => pool(sumOfFactors(v)))

  def sofI(i: Int): Task[Int] = Context.default(sumOfFactors(i)).map(v => sumOfFactors(v))
  def sofZ(i: Int): TaskZ[Int] = TaskZ(sumOfFactors(i)).map(v => sumOfFactors(v))
  def sofS(i: Int): FutureS[Int] = FutureS(sumOfFactors(i)).map(v => sumOfFactors(v))
  def sofT(i: Int): FutureT[Int] = pool(sumOfFactors(i)).map(v => sumOfFactors(v))

  @Benchmark
  def traverse0I: List[Int] = Task.taskInstance.gather(range.map(sofI)).unsafePerformSync

  @Benchmark
  def traverse1Z: List[Int] = TaskZ.taskInstance.gather(range.map(sofZ)).unsafePerformSync

  @Benchmark
  def traverse2S: Vector[Int] = Await.result(FutureS.traverse(range)(sofS), Duration.Inf)

  @Benchmark
  def traverse3T: Seq[Int] = AwaitT.result(FutureT.collect(range.map(sofT)))
}

/**
 * Compare the performance of iteratee operations.
 *
 * The following command will run the benchmarks with reasonable settings:
 *
 * > sbt "benchmark/jmh:run -i 10 -wi 10 -f 2 -t 1 io.travisbrown.concurrent.benchmark.TraverseBenchmark"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class FutureBenchmark {

  val range: Vector[Int] = (100 to 10100).toVector

  implicit val es: java.util.concurrent.ExecutorService = new java.util.concurrent.ForkJoinPool()

  def sumOfFactors(n: Int): Int = (1 to n)/*.filter(i => n % i == 0)*/.sum

  def sofI(i: Int): Future[Int] = Context.default.future(sumOfFactors(i)).map(sumOfFactors)
  def sofZ(i: Int): FutureZ[Int] = FutureZ(sumOfFactors(i)).map(sumOfFactors)

  @Benchmark
  def traverseI: List[Int] = Future.futureInstance.gather(range.map(sofI)).unsafePerformSync

  @Benchmark
  def traverseZ: List[Int] = FutureZ.futureInstance.gather(range.map(sofZ)).unsafePerformSync
}

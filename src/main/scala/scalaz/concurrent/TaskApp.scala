package scalaz.concurrent

/** Safe `App` trait that runs a `scalaz.concurrent.Task` action.
  *
  * Clients should implement `run`, `runl`, or `runc`.
  */
trait TaskApp {
  def run(args: Vector[String]): Task[Unit] = runl(args.toList)

  def runl(args: List[String]): Task[Unit] = runc

  def runc: Task[Unit] = Task.now(())

  final def main(args: Array[String]): Unit =
    run(args.toVector).unsafePerformSync
}

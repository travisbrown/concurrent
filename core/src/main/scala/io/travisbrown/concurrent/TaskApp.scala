package io.travisbrown.concurrent

/** Safe `App` trait that runs a [[Task]] action.
  *
  * Clients should implement `run`.
  */
trait TaskApp {
  def run(args: List[String]): Task[Unit]

  final def main(args: Array[String]): Unit = run(args.toList).unsafePerformSync
}

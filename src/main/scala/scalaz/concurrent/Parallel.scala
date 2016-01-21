package scalaz.concurrent

class Parallel[F[_], A](val underlying: F[A]) extends AnyVal

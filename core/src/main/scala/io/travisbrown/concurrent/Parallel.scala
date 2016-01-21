package io.travisbrown.concurrent

final class Parallel[F[_], A](val underlying: F[A])

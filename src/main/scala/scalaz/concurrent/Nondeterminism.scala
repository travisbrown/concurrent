package scalaz.concurrent

import cats.{ Applicative, Foldable, MonoidK, Monad, Monoid, Semigroup }
import cats.data.{ NonEmptyList, OneAnd, Xor }
import cats.std.list._
import cats.syntax.foldable._

////
/**
 * A context supporting nondeterministic choice. Unlike `Monad.bind`,
 * which imposes a total order on the sequencing of effects throughout a
 * computation, the `choose` and `chooseAny` operations let us
 * partially order the sequencing of effects. Canonical instances are
 * `concurrent.Future` and `concurrent.Task`, which run their arguments
 * in parallel, returning whichever comes back 'first'.
 *
 * TODO - laws
 */
////
trait Nondeterminism[F[_]] extends Monad[F] { self =>
  ////

  /**
   * A commutative operation which chooses nondeterministically to obtain
   * a value from either `a` or `b`. If `a` 'wins', a 'residual' context
   * for `b` is returned; if `b` wins, a residual context for `a` is
   * returned. The residual is useful for various instances like `Future`,
   * which may race the two computations and require a residual to ensure
   * the result of the 'losing' computation is not discarded.
   *
   * This function can be defined in terms of `chooseAny` or vice versa.
   * The default implementation calls `chooseAny` with a
   * two-element list and uses the `Functor` for `F` to fix up types.
   */
  def choose[A,B](a: F[A], b: F[B]): F[Xor[(A, F[B]), (F[A], B)]] =
    map(chooseAny(List[F[Xor[A, B]]](map(a)(Xor.left), map(b)(Xor.right))).get) {
      (x: (Xor[A, B], Seq[F[Xor[A, B]]])) => x match {
        case (Xor.Left(a), Seq(br)) =>
          Xor.left((a, map(br) {
            case Xor.Right(b) => b
            case _ => sys.error("broken residual handling in a Nondeterminism instance")
          }))
        case (Xor.Right(b), Seq(ar)) =>
          Xor.right((map(ar) {
            case Xor.Left(a) => a
            case _ => sys.error("broken residual handling in a Nondeterminism instance")
          }, b))
        case _ => sys.error("broken Nondeterminism instance tossed out a residual")
      }
    }

  /**
   * A commutative operation which chooses nondeterministically to obtain
   * a value from any of the elements of `as`. In the language of posets, this
   * constructs an antichain (a set of elements which are all incomparable) in
   * the effect poset for this computation.
   *
   * @return `None`, if the input is empty.
   */
  def chooseAny[A](a: Seq[F[A]]): Option[F[(A, Seq[F[A]])]] =
    if (a.isEmpty) None
    else Some(chooseAny(a.head, a.tail))

  def chooseAny[A](head: F[A], tail: Seq[F[A]]): F[(A, Seq[F[A]])]

  // derived functions

  /**
   * Apply a function to the results of `a` and `b`, nondeterminstically
   * ordering their effects.
   */
  def mapBoth[A,B,C](a: F[A], b: F[B])(f: (A,B) => C): F[C] =
    flatMap(choose(a, b)) {
      case Xor.Left((a,rb)) => map(rb)(b => f(a,b))
      case Xor.Right((ra,b)) => map(ra)(a => f(a,b))
    }


  /**
   * Apply a function to 2 results, nondeterminstically ordering their effects, alias of mapBoth
   */
  def nmap2[A,B,C](a: F[A], b: F[B])(f: (A,B) => C): F[C] =
    mapBoth(a,b)(f)

  /**
   * Apply a function to 3 results, nondeterminstically ordering their effects 
   */
  def nmap3[A,B,C,R](a: F[A], b: F[B], c: F[C])(f: (A,B,C) => R): F[R] =
    nmap2(nmap2(a, b)((_,_)), c)((ab,c) => f(ab._1, ab._2, c))

  /**
   * Apply a function to 4 results, nondeterminstically ordering their effects 
   */
  def nmap4[A,B,C,D,R](a: F[A], b: F[B], c: F[C], d: F[D])(f: (A,B,C,D) => R): F[R] =
    nmap2(nmap2(a, b)((_,_)), nmap2(c,d)((_,_)))((ab,cd) => f(ab._1, ab._2, cd._1, cd._2))

  /**
   * Apply a function to 5 results, nondeterminstically ordering their effects 
   */
  def nmap5[A,B,C,D,E,R](a: F[A], b: F[B], c: F[C], d: F[D], e: F[E])(f: (A,B,C,D,E) => R): F[R] =
    nmap2(nmap2(a, b)((_,_)), nmap3(c,d,e)((_,_,_)))((ab,cde) => f(ab._1, ab._2, cde._1, cde._2, cde._3))

  /**
   * Apply a function to 6 results, nondeterminstically ordering their effects 
   */
  def nmap6[A,B,C,D,E,FF,R](a: F[A], b: F[B], c: F[C], d: F[D], e: F[E], ff:F[FF])(f: (A,B,C,D,E,FF) => R): F[R] =
    nmap2(nmap3(a, b, c)((_,_,_)), nmap3(d,e,ff)((_,_,_)))((abc,deff) => f(abc._1, abc._2, abc._3, deff._1, deff._2, deff._3))


  /**
   * Obtain results from both `a` and `b`, nondeterministically ordering
   * their effects.
   */
  def both[A,B](a: F[A], b: F[B]): F[(A,B)] = mapBoth(a,b)((_,_))

  /**
   * Nondeterministically gather results from the given sequence of actions
   * to a list. Same as calling `reduceUnordered` with the `List` `Monoid`.
   *
   * To preserve the order of the output list while allowing nondetermininstic
   * ordering of effects, use `gather`.
   */
  def gatherUnordered[A](fs: Seq[F[A]]): F[List[A]] =
    reduceUnordered[A, List](fs)

  def gatherUnordered1[A](fs: NonEmptyList[F[A]]): F[NonEmptyList[A]] = {
    flatMap(chooseAny(fs.head, fs.tail)) { case (a, residuals) =>
      map(reduceUnordered(residuals))(NonEmptyList(a, _))
    }
  }

  /**
   * Nondeterministically gather results from the given sequence of actions.
   * The result will be arbitrarily reordered, depending on the order
   * results come back in a sequence of calls to `chooseAny`.
   */
  def reduceUnordered[A, M[_]](fs: Seq[F[A]])(implicit R: MonoidK[M], M: Applicative[M]): F[M[A]] =
    if (fs.isEmpty) pure(R.empty)
    else flatMap(chooseAny(fs.head, fs.tail)) { case (a, residuals) =>
      map(reduceUnordered[A, M](residuals))(R.combine(M.pure(a), _))
    }

  /**
   * Nondeterministically gather results from the given sequence of actions.
   * This function is the nondeterministic analogue of `sequence` and should
   * behave identically to `sequence` so long as there is no interaction between
   * the effects being gathered. However, unlike `sequence`, which decides on
   * a total order of effects, the effects in a `gather` are unordered with
   * respect to each other.
   *
   * Although the effects are unordered, we ensure the order of results
   * matches the order of the input sequence. Also see `gatherUnordered`.
   */
  def gather[A](fs: Seq[F[A]]): F[List[A]] =
    map(gatherUnordered(fs.toList.zipWithIndex.map { case (f,i) => map(f)((_ ,i)) }))(
      ais => ais.sortBy(_._2).map(_._1))

  def gather1[A](fs: NonEmptyList[F[A]]): F[NonEmptyList[A]] =
    map(gatherUnordered1(NonEmptyList(map(fs.head)((_, -1)), fs.tail.zipWithIndex.map { case (f, i) => map(f)((_ ,i)) })))(
      ais => NonEmptyList.fromList(ais.toList.sortBy(_._2).map(_._1)).get)

  /**
   * Nondeterministically sequence `fs`, collecting the results using a `Monoid`.
   */
  def aggregate[A: Monoid](fs: Seq[F[A]]): F[A] =
    map(gather(fs))(_.foldLeft(implicitly[Monoid[A]].empty)((a,b) => implicitly[Monoid[A]].combine(a,b)))

  def aggregate1[A: Monoid /*Semigroup*/](fs: NonEmptyList[F[A]]): F[A] =
    map(gather1(fs))(Foldable[NonEmptyList].combineAll(_))

  /**
   * Nondeterministically sequence `fs`, collecting the results using
   * a commutative `Monoid`.
   */
  def aggregateCommutative[A: Monoid](fs: Seq[F[A]]): F[A] =
    map(gatherUnordered(fs))(_.foldLeft(implicitly[Monoid[A]].empty)((a,b) => implicitly[Monoid[A]].combine(a,b)))

  def aggregateCommutative1[A](fs: NonEmptyList[F[A]])(implicit A: Semigroup[A]): F[A] =
    map(gatherUnordered1(fs)) {
      case OneAnd(h, t) => t.foldLeft(h)(A.combine)
    }

  def parallel: Applicative[({ type L[x] = Parallel[F, x] })#L] =
    new Applicative[({ type L[x] = Parallel[F, x] })#L] {
      def pure[A](a: A) = new Parallel(self.pure(a))
      override def map[A, B](fa: Parallel[F, A])(f: A => B) =
        new Parallel(self.map(fa.underlying)(f))
      def ap[A, B](fa: Parallel[F, A])(fab: Parallel[F, A => B]) =
        new Parallel(self.mapBoth(fa.underlying, fab.underlying)((a, f) => f(a)))
      def product[A, B](fa: Parallel[F, A], fb: Parallel[F, B]): Parallel[F, (A, B)] = new Parallel(self.both(fa.underlying, fb.underlying))
    }
}

object Nondeterminism {
  @inline def apply[F[_]](implicit F: Nondeterminism[F]): Nondeterminism[F] = F

  ////

  ////
}

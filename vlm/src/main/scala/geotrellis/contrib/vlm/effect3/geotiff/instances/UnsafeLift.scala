package geotrellis.contrib.vlm.effect3.geotiff.instances

import cats.effect.IO

import scala.util.Try

/** Type class that allows to handle unsafe calls */
trait UnsafeLift[F[_]] {
  def apply[A](value: => A): F[A]
}

object UnsafeLift {
  def apply[F[_]: UnsafeLift]: UnsafeLift[F] = implicitly[UnsafeLift[F]]

  implicit val io: UnsafeLift[IO] = new UnsafeLift[IO] {
    def apply[A](value: => A): IO[A] = IO(value)
  }

  implicit val option: UnsafeLift[Option] = new UnsafeLift[Option] {
    def apply[A](value: => A): Option[A] = Try(value).toOption
  }
}

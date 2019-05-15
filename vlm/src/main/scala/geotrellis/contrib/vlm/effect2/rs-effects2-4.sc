import cats._
import cats.implicits._
import cats.effect._
import geotrellis.contrib.vlm.effect2.geotiff._
import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.Extent

import scala.concurrent.ExecutionContext

val uri = "/Users/daunnc/subversions/git/github/pomadchin/geotrellis-contrib/gdal/src/test/resources/img/aspect-tiled.tif"

object optcase {
  lazy val source: GeoTiffRasterSource[Option] = GeoTiffRasterSource[Option](uri)

  lazy val raster1: Option[Raster[MultibandTile]] = source.read(Extent(0, 0, 1, 1))
  lazy val raster2: Option[Raster[MultibandTile]] = source.read(Extent(630000.0, 215000.0, 639000.0, 219500.0))
  lazy val raster3: Option[Raster[MultibandTile]] = source.read()

  def run = (raster1, raster2, raster3)

}

object iocase {
  implicit lazy val cs = IO.contextShift(ExecutionContext.global)

  // run in a main thread
  // implicit val cs = IO.contextShift(ExecutionContext.fromExecutor(new RasterSourceF.CurrentThreadExecutor))

  /*val i = 1000
  val n = 200
  val pool = Executors.newFixedThreadPool(n, new ThreadFactoryBuilder().setNameFormat("scala-sheets-%d").build())
  val ec = ExecutionContext.fromExecutor(pool)
  implicit val cs = IO.contextShift(ec)*/

  lazy val source: GeoTiffRasterSource[IO] = GeoTiffRasterSource[IO](uri)

  lazy val raster1: IO[Raster[MultibandTile]] = source.read(/*Extent(0, 0, 1, 1)*/)
  lazy val raster2: IO[Raster[MultibandTile]] = source.read(Extent(630000.0, 215000.0, 639000.0, 219500.0))
  lazy val raster3: IO[Raster[MultibandTile]] = source.read()

  println(s"${Thread.currentThread().getName}")

  lazy val result: Either[Throwable, List[Raster[MultibandTile]]] =
    List(
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3,
      raster2, raster3
    ).zipWithIndex
      .map { case (io, idx) => // add a thread printing
        IO.shift *> io.flatMap { v =>
          IO.shift *> IO {
            println(s"${idx}: ${Thread.currentThread().getName}")
            v
          }
        }
      }
      .parSequence
      .attempt
      .unsafeRunSync()

  def run = result
}

optcase.run
iocase.run

/*val result: Either[Throwable, List[Raster[MultibandTile]]] =
List(
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3,
  raster2, raster3
).zipWithIndex
  .map { case (io, idx) => // add a thread printing
    io.flatMap { v =>
      IO {
        println(s"${idx}: ${Thread.currentThread().getName}")
        v
      }
    }
  }
  .sequence
  .attempt
  .unsafeRunSync()*/

/*val result: Either[Throwable, List[Raster[MultibandTile]]] =
List(raster2, raster3)
  .map { io => // add a thread printing
    io.flatMap { v => IO { println(Thread.currentThread().getName); v } }
  }
  .parSequence
  .attempt
  .unsafeRunSync()*/

/*val result: List[Either[Throwable, Raster[MultibandTile]]] =
List(raster1, raster2, raster3)
  .map { io => // add a thread printing
    io.flatMap { v => IO { println(Thread.currentThread().getName); v } }
  }
  .map(_.attempt.unsafeRunSync())*/

// println(result)

// val res = raster.attempt.unsafeRunSync()

// println(res)

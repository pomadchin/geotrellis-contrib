import cats._
import cats.effect._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.list._
import cats.syntax.traverse._
import cats.syntax.parallel._

import geotrellis.contrib.vlm.effect._
import geotrellis.contrib.vlm.effect.geotiff._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import geotrellis.raster.{MultibandTile, Raster}
import geotrellis.vector.Extent
import com.google.common.util.concurrent.ThreadFactoryBuilder
import geotrellis.contrib.vlm.effect.RasterSourceF

// implicit val cs = IO.contextShift(ExecutionContext.global)

// run in a main thread
// implicit val cs = IO.contextShift(ExecutionContext.fromExecutor(new RasterSourceF.CurrentThreadExecutor))

/*val i = 1000
val n = 200
val pool = Executors.newFixedThreadPool(n, new ThreadFactoryBuilder().setNameFormat("scala-sheets-%d").build())
val ec = ExecutionContext.fromExecutor(pool)
implicit val cs = IO.contextShift(ec)*/

val uri = "/Users/daunnc/subversions/git/github/pomadchin/geotrellis-contrib/gdal/src/test/resources/img/aspect-tiled.tif"
val source: GeoTiffRasterSource[IO] = GeoTiffRasterSource.sync(uri)

val raster1: IO[Raster[MultibandTile]] = source.read(Extent(0, 0, 1, 1))
val raster2: IO[Raster[MultibandTile]] = source.read(Extent(630000.0, 215000.0, 639000.0, 219500.0))
val raster3: IO[Raster[MultibandTile]] = source.read()

println(Thread.currentThread().getName)

val result: Either[Throwable, List[Raster[MultibandTile]]] =
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
    .unsafeRunSync()

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

println(result)

// val res = raster.attempt.unsafeRunSync()

// println(res)

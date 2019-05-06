/*
 * Copyright 2019 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm.effect.geotiff

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.effect._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import cats.syntax.functor._
import cats.syntax.apply._
import cats.effect._

import scala.concurrent.ExecutionContext

case class GeoTiffRasterSource[F[_]](
  uri: String,
  private[vlm] val targetCellType: Option[TargetCellType] = None
)(implicit val F: Sync[F], implicit val cs: ContextShift[F]) extends RasterSourceF[F] {
  def resampleMethod: Option[ResampleMethod] = None

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  lazy val gridExtent: GridExtent[Long] = tiff.rasterExtent.toGridType[Long]
  lazy val resolutions: List[GridExtent[Long]] = gridExtent :: tiff.overviews.map(_.rasterExtent.toGridType[Long])

  def crs: CRS = tiff.crs
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffRasterSource[F] =
    GeoTiffRasterSource(uri, Some(targetCellType))

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] =
    cs.shift *> F.delay {
      RasterSourceF.synchronized {
        val bounds = gridExtent.gridBoundsFor(extent, clamp = false).toGridType[Int]
        val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
        val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
          // TODO: shouldn't GridExtent give me Extent for types other than N ?
          Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = false))
        }
        if (it.isEmpty) throw new Exception("The requested extent has no intersections with the actual RasterSource")
        else convertRaster(it.next)
      }
    }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    cs.shift *> F.defer { readBounds(List(bounds), bands).map(_.next) }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    cs.shift *> F.defer {
      val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = true))
      readBounds(bounds, bands)
    }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    cs.shift *> F.delay {
      val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
      val intersectingBounds: Seq[GridBounds[Int]] =
        bounds.flatMap(_.intersection(this.gridBounds)).
          toSeq.map(b => b.toGridType[Int])

      RasterSourceF.synchronized {
        geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
          convertRaster(Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = true)))
        }
      }
    }
}

object GeoTiffRasterSource {
  def sync(uri: String): GeoTiffRasterSource[IO] = {
    implicit val cs = IO.contextShift(ExecutionContext.fromExecutor(new RasterSourceF.CurrentThreadExecutor))
    GeoTiffRasterSource[IO](uri)
  }

  def async[F[_]: Sync: ContextShift](uri: String): GeoTiffRasterSource[F] = GeoTiffRasterSource(uri)
}

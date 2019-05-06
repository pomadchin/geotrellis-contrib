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
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, GeoTiffMultibandTile, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject.{Reproject, ReprojectRasterExtent}
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.vector._

import cats._
import cats.syntax.functor._
import cats.syntax.apply._
import cats.effect._

case class GeoTiffResampleRasterSource[F[_]](
  uri: String,
  resampleGrid: ResampleGrid[Long],
  method: ResampleMethod = NearestNeighbor,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
)(implicit val F: Sync[F], val cs: ContextShift[F]) extends RasterSourceF[F] { self =>
  def resampleMethod: Option[ResampleMethod] = Some(method)

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  def crs: CRS = tiff.crs
  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)

  override lazy val gridExtent: GridExtent[Long] = resampleGrid(tiff.rasterExtent.toGridType[Long])
  lazy val resolutions: List[GridExtent[Long]] = {
      val ratio = gridExtent.cellSize.resolution / tiff.rasterExtent.cellSize.resolution
      gridExtent :: tiff.overviews.map { ovr =>
        val re = ovr.rasterExtent
        val CellSize(cw, ch) = re.cellSize
        new GridExtent[Long](re.extent, CellSize(cw * ratio, ch * ratio))
      }
    }

  @transient protected lazy val closestTiffOverview: GeoTiff[MultibandTile] =
    tiff.getClosestOverview(gridExtent.cellSize, strategy)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    new GeoTiffReprojectRasterSource[F](uri, targetCRS, reprojectOptions, strategy, targetCellType) {
      override lazy val gridExtent: GridExtent[Long] = reprojectOptions.targetRasterExtent match {
        case Some(targetRasterExtent) => targetRasterExtent.toGridType[Long]
        case None => ReprojectRasterExtent(self.gridExtent, this.transform, this.reprojectOptions)
      }
    }

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffResampleRasterSource[F] =
    GeoTiffResampleRasterSource(uri, resampleGrid, method, strategy, Some(targetCellType))

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] =
    cs.shift *> F.defer {
      val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
      readBounds(List(bounds), bands).map(_.next)
    }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    cs.shift *> F.defer { readBounds(List(bounds), bands).map(_.next) }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] = {
    val targetPixelBounds = extents.map(gridExtent.gridBoundsFor(_))
    // result extents may actually expand to cover pixels at our resolution
    // TODO: verify the logic here, should the sourcePixelBounds be calculated from input or expanded extent?
    readBounds(targetPixelBounds, bands)
  }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    cs.shift *> F.delay {
      RasterSourceF.synchronized {
        val geoTiffTile = closestTiffOverview.tile.asInstanceOf[GeoTiffMultibandTile]

        val windows = { for {
          queryPixelBounds <- bounds
          targetPixelBounds <- queryPixelBounds.intersection(this.gridBounds)
        } yield {
          val targetExtent = gridExtent.extentFor(targetPixelBounds)
          val sourcePixelBounds = closestTiffOverview.rasterExtent.gridBoundsFor(targetExtent, clamp = true)
          val targetRasterExtent = RasterExtent(targetExtent, targetPixelBounds.width.toInt, targetPixelBounds.height.toInt)
          (sourcePixelBounds, targetRasterExtent)
        }}.toMap

        geoTiffTile.crop(windows.keys.toSeq, bands.toArray).map { case (gb, tile) =>
          val targetRasterExtent = windows(gb)
          Raster(
            tile = tile,
            extent = targetRasterExtent.extent
          ).resample(targetRasterExtent.cols, targetRasterExtent.rows, method)
        }
      }
    }
}

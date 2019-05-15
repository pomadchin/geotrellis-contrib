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

package geotrellis.contrib.vlm.effect2.geotiff

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.effect2._
import geotrellis.contrib.vlm.effect2.geotiff.syntax._
import geotrellis.contrib.vlm.effect2.geotiff.instances._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.{AutoHigherResolution, GeoTiff, MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.vector._

import cats.Applicative
import cats.syntax.functor._

case class GeoTiffReprojectRasterSource[F[_]](
  uri: String,
  crs: CRS,
  reprojectOptions: Reproject.Options = Reproject.Options.DEFAULT,
  strategy: OverviewStrategy = AutoHigherResolution,
  private[vlm] val targetCellType: Option[TargetCellType] = None
)(implicit val F: Applicative[F], implicit val reader: GeoTiffMultibandReader[F]) extends RasterSourceF[F] {
  def resampleMethod: Option[ResampleMethod] = Some(reprojectOptions.method)

  @transient lazy val tiff: MultibandGeoTiff =
    GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)

  protected lazy val baseCRS: CRS = tiff.crs
  protected lazy val baseGridExtent: GridExtent[Long] = tiff.rasterExtent.toGridType[Long]

  protected lazy val transform = Transform(baseCRS, crs)
  protected lazy val backTransform = Transform(crs, baseCRS)

  override lazy val gridExtent: GridExtent[Long] = reprojectOptions.targetRasterExtent match {
      case Some(targetRasterExtent) =>
        targetRasterExtent.toGridType[Long]

        case None =>
        ReprojectRasterExtent(baseGridExtent, transform, reprojectOptions)
    }

  lazy val resolutions: List[GridExtent[Long]] =
      gridExtent :: tiff.overviews.map(ovr => ReprojectRasterExtent(ovr.rasterExtent.toGridType[Long], transform))

  @transient private[vlm] lazy val closestTiffOverview: GeoTiff[MultibandTile] = {
    if (reprojectOptions.targetRasterExtent.isDefined
      || reprojectOptions.parentGridExtent.isDefined
      || reprojectOptions.targetCellSize.isDefined)
    {
      // we're asked to match specific target resolution, estimate what resolution we need in source to sample it
      val estimatedSource = ReprojectRasterExtent(gridExtent, backTransform)
      tiff.getClosestOverview(estimatedSource.cellSize, strategy)
    } else {
      tiff.getClosestOverview(baseGridExtent.cellSize, strategy)
    }
  }

  def bandCount: Int = tiff.bandCount
  def cellType: CellType = dstCellType.getOrElse(tiff.cellType)

  def read(extent: Extent, bands: Seq[Int]): F[Raster[MultibandTile]] = {
      val bounds = gridExtent.gridBoundsFor(extent, clamp = false)
      readBounds(List(bounds), bands).map(_.next)
    }

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    reader.liftF { readBounds(List(bounds), bands).map(_.next) }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    reader.liftF {
      val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = true))
      readBounds(bounds, bands)
    }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    tiff.readBounds(bounds, bands, targetCellType)

  def reproject(targetCRS: CRS, reprojectOptions: Reproject.Options, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(uri, targetCRS, reprojectOptions, strategy, targetCellType)

  def resample(resampleGrid: ResampleGrid[Long], method: ResampleMethod, strategy: OverviewStrategy): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(uri, crs, reprojectOptions.copy(method = method, targetRasterExtent = Some(resampleGrid(this.gridExtent).toRasterExtent)), strategy, targetCellType)

  def convert(targetCellType: TargetCellType): GeoTiffReprojectRasterSource[F] =
    GeoTiffReprojectRasterSource(uri, crs, reprojectOptions, strategy, Some(targetCellType))
}

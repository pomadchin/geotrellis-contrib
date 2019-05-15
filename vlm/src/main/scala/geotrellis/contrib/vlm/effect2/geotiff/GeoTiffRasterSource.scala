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
import geotrellis.raster.io.geotiff.{MultibandGeoTiff, OverviewStrategy}
import geotrellis.raster.reproject.Reproject
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._

import cats.Applicative
import cats.syntax.functor._

case class GeoTiffRasterSource[F[_]](
  uri: String,
  private[vlm] val targetCellType: Option[TargetCellType] = None
)(implicit val F: Applicative[F], implicit val reader: GeoTiffMultibandReader[F]) extends RasterSourceF[F] {
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
    tiff.read(extent, bands, targetCellType)

  def read(bounds: GridBounds[Long], bands: Seq[Int]): F[Raster[MultibandTile]] =
    reader.liftF { readBounds(List(bounds), bands).map(_.next) }

  override def readExtents(extents: Traversable[Extent], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    reader.liftF {
      val bounds = extents.map(gridExtent.gridBoundsFor(_, clamp = true))
      readBounds(bounds, bands)
    }

  override def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int]): F[Iterator[Raster[MultibandTile]]] =
    tiff.readBounds(bounds, bands, targetCellType)
}


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

package geotrellis.contrib.vlm.effect2.geotiff.instances

import geotrellis.contrib.vlm.TargetCellType
import geotrellis.contrib.vlm.effect2.RasterSourceF

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.vector._
import cats.effect.Sync

class GeoTiffMultibandReaderSync[F[_]](implicit F: Sync[F]) extends GeoTiffMultibandReader[F] {
  def lift[A](a: => A): F[A] = F.delay(a)
  def liftF[A](a: => F[A]): F[A] = F.defer(a)

  def read(extent: Extent, bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): F[Raster[MultibandTile]] = F.delay {
    RasterSourceF.synchronized {
      val gridExtent = tiff.rasterExtent.toGridType[Long]
      val bounds = gridExtent.gridBoundsFor(extent, clamp = false).toGridType[Int]
      val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
      val it = geoTiffTile.crop(List(bounds), bands.toArray).map { case (gb, tile) =>
        Raster(tile, gridExtent.extentFor(gb.toGridType[Long], clamp = false))
      }
      if (it.isEmpty) throw new Exception("The requested extent has no intersections with the actual RasterSource")
      else convertRaster(it.next, targetCellType)
    }
  }

  def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): F[Iterator[Raster[MultibandTile]]] = F.delay {
    val geoTiffTile = tiff.tile.asInstanceOf[GeoTiffMultibandTile]
    val intersectingBounds: Seq[GridBounds[Int]] =
      bounds.flatMap(_.intersection(tiff.rasterExtent.toGridType[Long].gridBounds)).toSeq.map(_.toGridType[Int])

    RasterSourceF.synchronized {
      geoTiffTile.crop(intersectingBounds, bands.toArray).map { case (gb, tile) =>
        convertRaster(Raster(tile, tiff.rasterExtent.toGridType[Long].extentFor(gb.toGridType[Long], clamp = true)), targetCellType)
      }
    }
  }
}

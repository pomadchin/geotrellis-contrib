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

import geotrellis.contrib.vlm.{ConvertTargetCellType, TargetCellType}
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.vector._

import cats.effect.IO

trait GeoTiffMultibandReader[F[_]] {
  def read(extent: Extent, bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): F[Raster[MultibandTile]]
  def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): F[Iterator[Raster[MultibandTile]]]
  def lift[A](a: => A): F[A]
  def liftF[A](a: => F[A]): F[A]

  def convertRaster(raster: Raster[MultibandTile], targetCellType: Option[TargetCellType]):Raster[MultibandTile] =
    targetCellType match {
      case Some(target: ConvertTargetCellType) => target(raster)
      case _ => raster
    }

}

object GeoTiffMultibandReader {
  implicit val geoTiffMultibandReaderIO: GeoTiffMultibandReaderSync[IO] = new GeoTiffMultibandReaderSync[IO]
  implicit val geoTiffMultibandReaderId: GeoTiffMultibandReaderOption = new GeoTiffMultibandReaderOption
}

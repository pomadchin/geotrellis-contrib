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
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.vector._
import cats.syntax.either._

import scala.concurrent.duration.Duration

class GeoTiffMultibandReaderOption extends GeoTiffMultibandReader[Option] {
  import GeoTiffMultibandReader._

  def lift[A](a: => A): Option[A] = Option(a)
  def liftF[A](a: => Option[A]): Option[A] = a

  def read(extent: Extent, bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): Option[Raster[MultibandTile]] =
    geoTiffMultibandReaderIO.read(extent, bands, targetCellType).attempt.map(_.toOption).unsafeRunTimed(Duration.Inf).flatten

  def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int], targetCellType: Option[TargetCellType])(implicit tiff: MultibandGeoTiff): Option[Iterator[Raster[MultibandTile]]] =
    geoTiffMultibandReaderIO.readBounds(bounds, bands, targetCellType).attempt.map(_.toOption).unsafeRunTimed(Duration.Inf).flatten
}

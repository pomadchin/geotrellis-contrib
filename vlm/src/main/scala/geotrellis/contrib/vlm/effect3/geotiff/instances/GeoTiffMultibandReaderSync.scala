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

package geotrellis.contrib.vlm.effect3.geotiff.instances

import geotrellis.contrib.vlm.getByteReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

import cats.Monad
import cats.effect.Sync

class GeoTiffMultibandReaderSync[F[_]: Sync](implicit F: Monad[F]) extends GeoTiffMultibandReader[F] {
  def read(uri: String): F[MultibandGeoTiff] = Sync[F].delay(GeoTiffReader.readMultiband(getByteReader(uri), streaming = true))
}

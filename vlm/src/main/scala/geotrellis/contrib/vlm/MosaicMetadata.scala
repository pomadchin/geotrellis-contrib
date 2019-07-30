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

package geotrellis.contrib.vlm

import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, GridExtent}

import cats.instances.string._
import cats.instances.map._
import cats.data.NonEmptyList
import cats.Monad
import cats.syntax.apply._

case class MosaicMetadata(
  list: NonEmptyList[SourceMetadata],
  crs: CRS,
  bandCount: Int,
  cellType: CellType,
  gridExtent: GridExtent[Long],
  resolutions: List[GridExtent[Long]]
) extends SourceMetadata {
  def sourceMetadata(): Map[String, String] = list.map(_.sourceMetadata()).reduce
  def sourceMetadata(b: Int): Map[String, String] = if(b == 0) sourceMetadata() else list.map(_.sourceMetadata(b)).reduce
}

object MosaicMetadata {
  def apply[F[_]: Monad](
    list: F[NonEmptyList[SourceMetadata]],
    crs: F[CRS],
    bandCount: F[Int],
    cellType: F[CellType],
    gridExtent: F[GridExtent[Long]],
    resolutions: F[List[GridExtent[Long]]]
  ): F[MosaicMetadata] =
    (list, crs, bandCount, cellType, gridExtent, resolutions).mapN(MosaicMetadata.apply)
}

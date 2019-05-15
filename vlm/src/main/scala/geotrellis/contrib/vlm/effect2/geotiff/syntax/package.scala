package geotrellis.contrib.vlm.effect2.geotiff

import geotrellis.contrib.vlm.TargetCellType
import geotrellis.contrib.vlm.effect2.geotiff.instances.GeoTiffMultibandReader

import geotrellis.raster.{GridBounds, MultibandTile, Raster}
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector.Extent

package object syntax {
  implicit class GeoTiffMultibandReaderOps[F[_]](val tiff: MultibandGeoTiff)(implicit val reader: GeoTiffMultibandReader[F]) {
    def read(extent: Extent, bands: Seq[Int], targetCellType: Option[TargetCellType]): F[Raster[MultibandTile]] =
      reader.read(extent, bands, targetCellType)(tiff)

    def readBounds(bounds: Traversable[GridBounds[Long]], bands: Seq[Int], targetCellType: Option[TargetCellType]): F[Iterator[Raster[MultibandTile]]] =
      reader.readBounds(bounds, bands, targetCellType)(tiff)
  }

}

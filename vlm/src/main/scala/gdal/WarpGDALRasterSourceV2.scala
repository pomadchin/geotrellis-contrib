package geotrellis.contrib.vlm.gdal

import geotrellis.contrib.vlm.RasterSource
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample.{NearestNeighbor, ResampleMethod}
import geotrellis.proj4._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.tiling.LayoutDefinition
import org.gdal.gdal._
import org.gdal.osr.SpatialReference

case class WarpGDALRasterSourceV2(
  uri: String,
  crs: CRS,
  layout: LayoutDefinition,
  resampleMethod: ResampleMethod = NearestNeighbor,
  errorThreshold: Double = 0.125
) extends RasterSource {
  val baseSpatialReference = {
    val baseDataset: Dataset = GDAL.open(uri)

    val spatialReference = new SpatialReference(baseDataset.GetProjection)

    baseDataset.delete
    spatialReference
  }

  val targetSpatialReference: SpatialReference = {
    val spatialReference = new SpatialReference()
    spatialReference.ImportFromProj4(crs.toProj4String)
    spatialReference
  }

  final val warpParametersSet = {
    println(s"-tr ${layout.cellwidth} ${layout.cellheight}")
    new java.util.Vector(
      java.util.Arrays.asList(
        "-of", "VRT",
        "-s_srs", baseSpatialReference.ExportToProj4,
        "-t_srs", targetSpatialReference.ExportToProj4,
        "-tr", "19.2", "19.2", // layout.cellwidth.toString, layout.cellheight.toString,
        "-r", s"${GDAL.deriveResampleMethodString(resampleMethod)}",
        "-et", s"$errorThreshold"
      )
    )
  }

  @transient private lazy val vrt: Dataset = {
    // For some reason, baseDataset can't be in
    // the scope of the class or else a RunTime
    // Error will be encountered. So it is
    // created and closed when needed.
    val baseDataset: Dataset = GDAL.open(uri)

    val options = new WarpOptions(warpParametersSet)

    val dataset = gdal.Warp("", Array(baseDataset), options)

    baseDataset.delete

    dataset
  }


  @transient lazy val ll: Dataset = {
    lazy val dataset: Dataset = GDAL.open(uri)
    val sourceReference = baseSpatialReference
    val destinationReference = targetSpatialReference
    // If the source reference is not the same as the destination reference
    // lets warp the image using GDAL.
    // println(s"sourceReference.IsSame(destinationReference): ${sourceReference.IsSame(destinationReference)}")
    // if (sourceReference != destinationReference) {
      gdal.AutoCreateWarpedVRT(
        dataset,
        dataset.GetProjection,
        destinationReference.ExportToWkt(),
        GDAL.deriveGDALResampleMethod(resampleMethod),
        errorThreshold
      )
    // } else dataset
  }

  private lazy val colsLong: Long = vrt.getRasterXSize
  private lazy val rowsLong: Long = vrt.getRasterYSize

  def cols: Int = colsLong.toInt
  def rows: Int = rowsLong.toInt

  lazy val geoTransform: Array[Double] = vrt.GetGeoTransform
  lazy val invGeoTransofrm: Array[Double] = gdal.InvGeoTransform(geoTransform)

  lazy val pixelSizeX = geoTransform(1)
  lazy val pixelSizeY = - geoTransform(5)

  println("=================================")
  println(s"pixelSizeX, pixelSizeY: ${(pixelSizeX, pixelSizeY)}")
  println("=================================")

  private lazy val xmin: Double = geoTransform(0)
  private lazy val ymin: Double = geoTransform(3) + geoTransform(5) * rows
  private lazy val xmax: Double = geoTransform(0) + geoTransform(1) * cols
  private lazy val ymax: Double = geoTransform(3)

  lazy val extent = Extent(xmin, ymin, xmax, ymax)
  /*override lazy val rasterExtent =
    RasterExtent(
      extent,
      geoTransform(1),
      math.abs(geoTransform(5)),
      cols,
      rows
    )*/

  lazy val bandCount: Int = vrt.getRasterCount

  private lazy val datatype: GDALDataType = {
    val band = vrt.GetRasterBand(1)
    band.getDataType()
  }

  lazy val cellType: CellType = GDAL.deriveGTCellType(datatype)

  private lazy val reader = GDALReader(vrt)

  def read(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val bounds: Map[GridBounds, RasterExtent] =
      windows.map { targetRasterExtent =>
        val targetExtent = extent.intersection(targetRasterExtent.extent).get

        // targetExtent.intersection(extent)

        val colMin = Array.ofDim[Double](1)
        val rowMin = Array.ofDim[Double](1)
        val colMax = Array.ofDim[Double](1)
        val rowMax = Array.ofDim[Double](1)

        gdal.ApplyGeoTransform(
          invGeoTransofrm,
          targetExtent.xmin,
          targetExtent.ymin,
          colMin,
          rowMax
        )

        gdal.ApplyGeoTransform(
          invGeoTransofrm,
          targetExtent.xmax,
          targetExtent.ymax,
          colMax,
          rowMin
        )

        // val existingRegion =
        GridBounds(
          colMin.head.toInt,
          rowMin.head.toInt,
          colMax.head.toInt,
          rowMax.head.toInt
        )

        val existingRegion = rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
        println(s"existingRegion.width -> existingRegion.height: ${existingRegion.width -> existingRegion.height}")
        println(s"existingRegion: ${existingRegion}")
        // println(s"existingRegionGT: ${existingRegionGT}")

        (existingRegion, targetRasterExtent)
      }.toMap

    bounds.map { case (gb, re) =>
      val initialTile = reader.read(gb)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val targetBounds = rasterExtent.gridBoundsFor(extent.intersection(re.extent).get, clamp = false)
          println(s"rasterExtent.cellSize: ${rasterExtent.cellSize}")
          println(s"gb: $gb")
          println(s"targetBounds: $targetBounds")
          println(s"targetBounds.height -> targetBounds.width: ${targetBounds.height -> targetBounds.width}")
          println(s"(re.cols, re.rows): ${(re.cols, re.rows)}")

          val updatedTiles = initialTile.bands.map { band =>
            val protoTile = band.prototype(re.cols, re.rows)

            println(s"band.cols -> band.rows: ${band.cols -> band.rows}")
            println(s"protoTile.update(${targetBounds.colMin - gb.colMin}, ${targetBounds.rowMin - gb.rowMin}, $band)")
            protoTile.update(targetBounds.colMin - gb.colMin, targetBounds.rowMin - gb.rowMin, band)
            protoTile
          }

          (targetBounds, MultibandTile(updatedTiles))
        } else
          (gb, initialTile)

      Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
  }

  def read2(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    val bounds: Map[GridBounds, RasterExtent] =
      windows.map { targetRasterExtent =>
        val targetExtent = targetRasterExtent.extent

        val colMin = Array.ofDim[Double](1)
        val rowMin = Array.ofDim[Double](1)
        val colMax = Array.ofDim[Double](1)
        val rowMax = Array.ofDim[Double](1)

        gdal.ApplyGeoTransform(
          invGeoTransofrm,
          targetExtent.xmin,
          targetExtent.ymin,
          colMin,
          rowMax
        )

        gdal.ApplyGeoTransform(
          invGeoTransofrm,
          targetExtent.xmax,
          targetExtent.ymax,
          colMax,
          rowMin
        )

        //val existingRegion =
          GridBounds(
            colMin.head.toInt,
            rowMin.head.toInt,
            colMax.head.toInt,
            rowMax.head.toInt
          )

        val existingRegion = rasterExtent.gridBoundsFor(targetRasterExtent.extent, clamp = true)
        println(s"existingRegion.width -> existingRegion.height: ${existingRegion.width -> existingRegion.height}")
        println(s"existingRegion: ${existingRegion}")

        (existingRegion, targetRasterExtent)
      }.toMap

    bounds.map { case (gb, re) =>
      println(s"gb.height -> gb.width: ${gb.height -> gb.width}")
      val initialTile = reader.read(gb)

      val (gridBounds, tile) =
        if (initialTile.cols != re.cols || initialTile.rows != re.rows) {
          val targetBounds = rasterExtent.gridBoundsFor(re.extent)
          println(s"gb: $gb")
          println(s"targetBounds: $targetBounds")

          val updatedTiles = initialTile.bands.map(_.resample(re.cols, re.rows)).map { band =>
            val protoTile = band.prototype(re.cols, re.rows)

            println(s"re.cols, re.rows: ${re.cols -> re.rows}")
            println(s"band.cols, band.rows: ${band.cols -> band.rows}")

            println(s"protoTile.update(${targetBounds.colMin - gb.colMin}, ${targetBounds.rowMin - gb.rowMin}, $band)")
            protoTile.update(math.max(0, targetBounds.colMin - gb.colMin), math.max(0, targetBounds.rowMin - gb.rowMin), band)
            protoTile
          }

          (targetBounds, MultibandTile(updatedTiles))
        } else
          (gb, initialTile)

      Raster(tile, rasterExtent.extentFor(gridBounds))
    }.toIterator
  }

  def read3(windows: Traversable[RasterExtent]): Iterator[Raster[MultibandTile]] = {
    windows.map { targetRasterExtent =>
      // The resulting bounds sometimes contains an extra col and/or row,
      // and it's not clear as to why. This needs to be fixed in order
      // to get this working.

      val targetExtent = targetRasterExtent.extent

      val colMin = Array.ofDim[Double](1)
      val rowMin = Array.ofDim[Double](1)
      val colMax = Array.ofDim[Double](1)
      val rowMax = Array.ofDim[Double](1)

      gdal.ApplyGeoTransform(
        invGeoTransofrm,
        targetExtent.xmin,
        targetExtent.ymin,
        colMin,
        rowMax
      )

      gdal.ApplyGeoTransform(
        invGeoTransofrm,
        targetExtent.xmax,
        targetExtent.ymax,
        colMax,
        rowMin
      )

      val bounds =
        GridBounds(
          colMin.head.toInt,
          rowMin.head.toInt,
          colMax.head.toInt,
          rowMax.head.toInt
        ) // .intersection(GridBounds(0, 0, vrt.getRasterXSize - 1, vrt.getRasterYSize - 1)).get

      val re = targetRasterExtent.withResolution(rasterExtent.cellwidth, rasterExtent.cellheight)

      println(s"\nThese are the bounds of the targetRasterExtent: ${targetRasterExtent.gridBounds}")
      println(s"These are the computed bounds: ${bounds}")
      println(s"More computed bounds: ${re.gridBounds}")
      println(s"width: ${re.gridBounds.width} height: ${re.gridBounds.height}")
      //println(s"width: ${r.gridBounds.width} height: ${r.gridBounds.height}")

      //val bufferXSize = math.ceil(targetRasterExtent.cellwidth).toInt
        //math.ceil(targetRasterExtent.cellwidth - rasterExtent.cellwidth).toInt
        //math.floor(bounds.width * targetRasterExtent.cellwidth).toInt

      //val bufferYSize = math.ceil(targetRasterExtent.cellheight).toInt
        //math.ceil(targetRasterExtent.cellheight - rasterExtent.cellheight).toInt
        //math.floor(bounds.height * targetRasterExtent.cellheight).toInt

      //println(s"\n\n !!!! bufferXSize: $bufferXSize bufferYSize: $bufferYSize !!! ")

      val tile = reader.read(targetRasterExtent.gridBounds)

      Raster(tile, targetRasterExtent.extent)
    }.toIterator
  }

  def withCRS(
    targetCRS: CRS,
    resampleMethod: ResampleMethod = NearestNeighbor
  ): WarpGDALRasterSource =
    WarpGDALRasterSource(uri, targetCRS, resampleMethod)
}

package org.apache.spark.sql.gt.sources

import java.net.URI

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark._
import geotrellis.spark.io.file.FileLayerReader
import geotrellis.spark.io.hadoop.HadoopLayerReader
import geotrellis.util.LazyLogging
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, gt}
import org.apache.spark.sql.gt.types._
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


case class GeoTrellisRelation(sqlContext: SQLContext, uri: URI, layerId: LayerId, bbox: Option[Extent])
  extends BaseRelation with PrunedFilteredScan with LazyLogging {

  // TODO: implement sizeInBytes

  override def schema: StructType = StructType(List(
    StructField("col", DataTypes.IntegerType, nullable =false),
    StructField("row", DataTypes.IntegerType, nullable =false),
    StructField("extent", StructType(List(
      StructField("xmin", DataTypes.DoubleType, nullable=false),
      StructField("xmax", DataTypes.DoubleType, nullable=false),
      StructField("ymin", DataTypes.DoubleType, nullable=false),
      StructField("ymax", DataTypes.DoubleType, nullable=false)
    ))),
    StructField("epsg", DataTypes.IntegerType, nullable =true),
    StructField("proj4", DataTypes.StringType, nullable =false),
    StructField("tile", TileUDT, nullable =true)
  ))

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array.empty[Filter])
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // only primitive comparisons can be pushed down, not UDFs
    // @see DataSourceStrategy.scala:509 translateFilter()
    filters
  }

  private def project(
    key: SpatialKey,
    tile: Tile,
    metadata: TileLayerMetadata[SpatialKey],
    column: String
  ): Any = column match {
    case "tile" => tile
    case "col" => key.col
    case "row" => key.row
    case "extent" => metadata.layout.mapTransform(key)
    case "epsg" => metadata.crs.epsgCode.orNull
    case "proj4" => metadata.crs.toProj4String
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logger.debug(s"Reading: $layerId from $uri")
    logger.debug(s"Required columns: ${requiredColumns.toList}")
    logger.debug(s"PushedDown filters: ${filters.toList}")

    implicit val sc = sqlContext.sparkContext

    // TODO: check they type of layer before reading, generating time column dynamically
    val reader = GeoTrellisRelation.layerReaderFromUri(uri)
    val query = reader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
    val rdd = bbox match {
      case Some(extent) => query.where(Intersects(extent)).result
      case None => query.result
    }
    val md = rdd.metadata
    rdd.map { case (key: SpatialKey, tile: Tile) =>
      val cols = requiredColumns.map(project(key, tile, md, _))
      Row(cols: _*)
    }
  }
}

object GeoTrellisRelation {
  def layerReaderFromUri(uri: URI)(implicit sc: SparkContext): FilteringLayerReader[LayerId] = {
    uri.getScheme match {
      case "file" =>
        FileLayerReader(uri.getSchemeSpecificPart)

      case "hdfs" =>
        val path = new org.apache.hadoop.fs.Path(uri)
        HadoopLayerReader(path)

      // others require modules outside of geotrellis-spark
    }
  }
}

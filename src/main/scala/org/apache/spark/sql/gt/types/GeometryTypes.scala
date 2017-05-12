package org.apache.spark.sql.gt.types

import geotrellis.vector._

/**
 * UDT for GT vector polygon
 *
 * @author sfitch 
 * @since 5/11/17
 */
private[gt] class PointUDT extends AbstractGeometryUDT[Point]("st_point")
case object PointUDT extends PointUDT

private[gt] class LineUDT extends AbstractGeometryUDT[Line]("st_line")
case object LineUDT extends LineUDT

private[gt] class MultiLineUDT extends AbstractGeometryUDT[MultiLine]("st_multiline")
case object MultiLineUDT extends MultiLineUDT

private[gt] class PolygonUDT extends AbstractGeometryUDT[Polygon]("st_polygon")
case object PolygonUDT extends PolygonUDT
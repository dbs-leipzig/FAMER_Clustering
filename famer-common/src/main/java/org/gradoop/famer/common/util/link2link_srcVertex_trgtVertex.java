package org.gradoop.famer.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.link2link_srcId_trgtId;
import org.gradoop.famer.common.functions.vertex2vertex_gradoopId;
import org.gradoop.famer.common.util.functions.gradoopId2vertexJoin1;
import org.gradoop.famer.common.util.functions.gradoopId2vertexJoin2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 *
 */
public class link2link_srcVertex_trgtVertex {
    private DataSet<Vertex> vertices;
    private DataSet<Edge> edges;
    public link2link_srcVertex_trgtVertex (LogicalGraph Graph){
        vertices = Graph.getVertices();
        edges = Graph.getEdges();
    }
    public link2link_srcVertex_trgtVertex (DataSet<Vertex> Vertices, DataSet<Edge> Edges){
        vertices = Vertices;
        edges = Edges;
    }
    public DataSet<Tuple3<Edge, Vertex, Vertex>> execute (){
        DataSet<Tuple2<Vertex, String>> vertex_gradoopId = vertices.map(new vertex2vertex_gradoopId());
        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = edges.map(new link2link_srcId_trgtId());
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = edge_srcId_trgtId.join(vertex_gradoopId).where(1).equalTo(1).
                with(new gradoopId2vertexJoin1())
                .join(vertex_gradoopId).where(2).equalTo(1).with(new gradoopId2vertexJoin2());
        return edge_srcVertex_trgtVertex;
    }
}

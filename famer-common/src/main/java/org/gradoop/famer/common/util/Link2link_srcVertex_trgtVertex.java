/*
 * Copyright © 2016 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gradoop.famer.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.functions.Link2link_srcId_trgtId;
import org.gradoop.famer.common.functions.Vertex2vertex_gradoopId;
import org.gradoop.famer.common.util.functions.GradoopId2vertexJoin1;
import org.gradoop.famer.common.util.functions.GradoopId2vertexJoin2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 *
 */
public class Link2link_srcVertex_trgtVertex {
    private DataSet<Vertex> vertices;
    private DataSet<Edge> edges;
    public Link2link_srcVertex_trgtVertex(LogicalGraph Graph){
        vertices = Graph.getVertices();
        edges = Graph.getEdges();
    }
    public Link2link_srcVertex_trgtVertex(DataSet<Vertex> Vertices, DataSet<Edge> Edges){
        vertices = Vertices;
        edges = Edges;
    }
    public DataSet<Tuple3<Edge, Vertex, Vertex>> execute (){
        DataSet<Tuple2<Vertex, String>> vertex_gradoopId = vertices.map(new Vertex2vertex_gradoopId());
        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = edges.map(new Link2link_srcId_trgtId());
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = edge_srcId_trgtId.join(vertex_gradoopId).where(1).equalTo(1).
                with(new GradoopId2vertexJoin1())
                .join(vertex_gradoopId).where(2).equalTo(1).with(new GradoopId2vertexJoin2());
        return edge_srcVertex_trgtVertex;
    }
}

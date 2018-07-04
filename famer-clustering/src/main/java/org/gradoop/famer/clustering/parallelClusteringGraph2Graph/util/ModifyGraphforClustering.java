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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

/**
 * Used to assign Vertex Priority to vertices.
 * It must be used before applying org.gradoop.famer.clustering.clustering algorithms on graphs
 * Notice: VertexPriority must not be 0
 */
public class ModifyGraphforClustering implements UnaryGraphToGraphOperator {
    public String getName() {
        return ModifyGraphforClustering.class.getName();
    }
    public LogicalGraph execute(final LogicalGraph input) {
        DataSet<Vertex> vertices = DataSetUtils.zipWithUniqueId(input.getVertices()).map(new MapFunction<Tuple2<Long, Vertex>, Vertex>() {
            public Vertex map(Tuple2<Long, Vertex> in) throws Exception {
                in.f1.setProperty("VertexPriority", in.f0+1);
                return in.f1;
            }
        });
        return input.getConfig().getLogicalGraphFactory().fromDataSets(vertices, input.getEdges());
    }

}

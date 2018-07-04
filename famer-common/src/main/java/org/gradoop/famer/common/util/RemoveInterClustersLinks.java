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
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class RemoveInterClustersLinks implements UnaryGraphToGraphOperator{
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_trgtVertex = new Link2link_srcVertex_trgtVertex(input).execute();
        DataSet<Edge> edges =  link_srcVertex_trgtVertex.flatMap(new org.gradoop.famer.common.util.functions.RemoveInterClustersLinks());
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }

    @Override
    public String getName() {
        return "RemoveInterClustersLinks";
    }
}

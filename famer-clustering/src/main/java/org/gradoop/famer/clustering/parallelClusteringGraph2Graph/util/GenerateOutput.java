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


import org.gradoop.famer.common.util.RemoveInterClustersLinks;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class GenerateOutput implements UnaryGraphToGraphOperator {
    private ClusteringOutputType clusteringOutputType;
    public GenerateOutput (ClusteringOutputType clusteringOutputType){
        this.clusteringOutputType = clusteringOutputType;
    }
    @Override
    public LogicalGraph execute(LogicalGraph input) {
        switch (clusteringOutputType){
            case VERTEX_SET:
                 return  input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices());
            case GRAPH:
                return input;
            case GRAPH_COLLECTION:
                return input.callForGraph(new RemoveInterClustersLinks());
        }
        return null;
    }

    @Override
    public String getName() {
        return GenerateOutput.class.getName();
    }
}

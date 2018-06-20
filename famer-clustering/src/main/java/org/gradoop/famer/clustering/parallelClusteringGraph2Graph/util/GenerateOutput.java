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

package org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class filterOverlappedVertices implements FlatMapFunction <Vertex, Vertex> {
    private Integer overlappingLength;
    public filterOverlappedVertices(Integer OverlappingLength){
        overlappingLength = OverlappingLength;
    }
    public filterOverlappedVertices(){
        overlappingLength = -1;
    }

    @Override
    public void flatMap(Vertex in, Collector<Vertex> out) throws Exception {
        String ClusterIds = in.getPropertyValue("ClusterId").toString();
        if (ClusterIds.contains(",")) {
            if(overlappingLength==2)
            for(String s:ClusterIds.split(","))
            System.out.println(s);
//            if (overlappingLength == -1)
                    out.collect(in);
//            else if (ClusterIds.split(",").length == overlappingLength)
//                out.collect(in);

        }
    }
}

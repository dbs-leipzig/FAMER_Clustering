package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class removeInterClustersLinks implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Edge>{
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> value, Collector<Edge> out) throws Exception {
        String[] clusterIds0 = value.f1.getPropertyValue("ClusterId").toString().split(",");
        String[] clusterIds1 = value.f2.getPropertyValue("ClusterId").toString().split(",");
        for (int i=0; i< clusterIds0.length; i++){
            for (int j=0; j< clusterIds1.length; j++){
                if (clusterIds0[i].equals(clusterIds1[j])){
                    out.collect(value.f0);
                    return;
                }
            }
        }

    }
}

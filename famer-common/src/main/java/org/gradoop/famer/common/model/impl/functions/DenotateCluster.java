package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.model.impl.Cluster;

import java.util.Collection;

/**
 */
public class DenotateCluster implements MapFunction <Cluster, Cluster>{
    private Integer sourceNo;
    public DenotateCluster(Integer SourceNo){sourceNo = SourceNo;}
    public Cluster map(Cluster in)  {
        in.setIsCompletePerfect(sourceNo);
        return in;
    }
}

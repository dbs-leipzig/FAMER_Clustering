package org.gradoop.famer.common.model.impl.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.*;

/**
 *
 */
public class classifyLinks implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex> , Tuple3<Edge, String, Boolean>> {
    @Override

    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Edge, String, Boolean>> out) throws Exception {
        List<String> srcClusterId = Arrays.asList(in.f1.getPropertyValue("ClusterId").toString().split(","));
        List<String> trgtClusterId = Arrays.asList(in.f2.getPropertyValue("ClusterId").toString().split(","));
        if (srcClusterId.size() == 1 && trgtClusterId.size()==1 && srcClusterId.get(0).equals(trgtClusterId.get(0))){ // intra
            out.collect(Tuple3.of(in.f0, srcClusterId.get(0), false));
            return;
        }
        List<String> union = findUnion(srcClusterId, trgtClusterId);
        for (String id:union){

            if (srcClusterId.contains(id) && trgtClusterId.contains(id) ) {// intra link
                out.collect(Tuple3.of(in.f0, id, false));
                if (srcClusterId.size() > 1 || trgtClusterId.size() >1) // inter
                    out.collect(Tuple3.of(in.f0, id, true));
            }
            else if ((srcClusterId.contains(id) && !trgtClusterId.contains(id)) || (!srcClusterId.contains(id) && trgtClusterId.contains(id))) // inter link
                out.collect(Tuple3.of(in.f0, id, true));
        }

    }
    private List<String> findUnion (List<String> first, List<String> second){
        List<String> outList = new ArrayList<>();
        for (String s:first){
            if (!outList.contains(s))
                outList.add(s);
        }
        for (String s:second){
            if (!outList.contains(s))
                outList.add(s);
        }

        return outList;
    }
}

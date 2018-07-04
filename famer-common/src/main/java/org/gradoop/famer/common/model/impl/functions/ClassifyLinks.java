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
public class ClassifyLinks implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex> , Tuple3<Edge, String, Boolean>> {
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

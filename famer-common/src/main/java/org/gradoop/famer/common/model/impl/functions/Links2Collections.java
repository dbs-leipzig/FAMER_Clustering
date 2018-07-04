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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class Links2Collections implements GroupReduceFunction<Tuple3<Edge, String, Boolean>, Tuple2<Collection<Tuple2<Edge, Boolean>>, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, String, Boolean>> in, Collector<Tuple2<Collection<Tuple2<Edge, Boolean>>, String>> out) throws Exception {
        Collection<Tuple2<Edge, Boolean>> edge_type = new ArrayList<>();
        String clusterId = "";
        for (Tuple3<Edge, String, Boolean> elem: in){
            if (!edge_type.contains(Tuple2.of(elem.f0, elem.f2)))
                edge_type.add(Tuple2.of(elem.f0, elem.f2));
            clusterId = elem.f1;
        }
        out.collect(Tuple2.of(edge_type,clusterId));
    }
}
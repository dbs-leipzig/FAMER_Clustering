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

package org.gradoop.famer.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;


/**
 */
public class Link2link_endId_endType implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, String, String>>{

    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> value, Collector<Tuple3<Edge, String, String>> out) throws Exception {
        out.collect(Tuple3.of(value.f0, value.f1.getId().toString(), value.f2.getPropertyValue("srcId").toString()));
        out.collect(Tuple3.of(value.f0, value.f2.getId().toString(), value.f1.getPropertyValue("srcId").toString()));
    }
}

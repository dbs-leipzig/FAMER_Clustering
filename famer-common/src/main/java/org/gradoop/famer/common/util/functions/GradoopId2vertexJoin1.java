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

package org.gradoop.famer.common.util.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class GradoopId2vertexJoin1 implements JoinFunction<Tuple3<Edge, String, String>, Tuple2<Vertex, String>, Tuple3<Edge, Vertex, String>> {
    @Override
    public Tuple3<Edge, Vertex, String> join(Tuple3<Edge, String, String> first, Tuple2<Vertex, String> second) throws Exception {
        return Tuple3.of(first.f0, second.f0, first.f2);
    }
}

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

package org.gradoop.famer.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f1;f1->f2")
public class Vertex_T2vertexId_vertex_T<T> implements MapFunction<Tuple2<Vertex, T>, Tuple3<String, Vertex, T>> {
    @Override
    public Tuple3<String, Vertex, T> map(Tuple2<Vertex, T> in) throws Exception {
        return Tuple3.of(in.f0.getId().toString(), in.f0, in.f1);
    }
}

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

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 */
public class Join1 implements JoinFunction <Tuple3<String, String, String>, Tuple4<String, String, String, Double>, Tuple5<String, String, String, String, Double>>{
    @Override
    public Tuple5<String, String, String, String, Double> join(Tuple3<String, String, String> in1, Tuple4<String, String, String, Double> in2) throws Exception {
        return Tuple5.of(in1.f1, in2.f0, in2.f1, in2.f2, in2.f3);
    }
}

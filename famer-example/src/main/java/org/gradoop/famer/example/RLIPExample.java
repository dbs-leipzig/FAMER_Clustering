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

package org.gradoop.famer.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.clusterPostProcessing.clusterRepair.OverlapResolve.OverlapResolve;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.CLIP;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 */
public class RLIPExample {
    public static void main(String args[]) throws Exception {

        String inputGraphPath = args[0];
        String outputGraphPath = args[1];
        Integer srcNo = Integer.parseInt(args[2]);
        Boolean hasOverlap = Boolean.parseBoolean(args[3]);
        new RLIPExample().execute(inputGraphPath, outputGraphPath, srcNo, hasOverlap);
    }

    public void execute(String srcFolder, String outFolder, Integer srcNo, Boolean hasOverlap) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig((ExecutionEnvironment) env);

        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        /***************************
         * Load Similarity Graph
         ********************************/
        JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", grad_config);
        LogicalGraph input = dataSource.getLogicalGraph();
        /***************************
         * Repairing
         ********************************/
        LogicalGraph resultGraph;
        LogicalGraph test = input;

        if (hasOverlap)
            test = test.callForGraph(new OverlapResolve(0d));
        resultGraph = test.callForGraph(new CLIP(clipConfig, ClusteringOutputType.GRAPH_COLLECTION));


        /***************************
         * Write Output Graph
         ********************************/

        DateFormat dateFormat = new SimpleDateFormat("MMdd_HHmm");
        Date date = new Date();
        String timestamp = dateFormat.format(date);
        resultGraph.writeTo(new JSONDataSink(outFolder + timestamp + "/graphHeads.json", outFolder + timestamp + "/vertices.json", outFolder + timestamp + "/edges.json", grad_config));


        /***************************
         * Compute Output Precision, Recall, FMeasure
         ********************************/
        FileWriter fw = new FileWriter(outFolder+"quality.csv",true);
        fw.append("Pre,Rec,FM\n");
        ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures(resultGraph, "recId",  false);
        fw.append(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+"\n");
        fw.flush();
    }
}

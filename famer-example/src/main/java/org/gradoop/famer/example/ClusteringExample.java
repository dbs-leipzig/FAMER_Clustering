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
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.*;


import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ClusteringExample {

	public enum ClusteringMethods {
		CONCON, CLIP
	};

	public static void main(String args[]) throws Exception {
		// Clustering method can be chosen from the above list
		ClusteringMethods method = ClusteringMethods.CLIP;
		// input graphs for different datasets are stored in "inputGraphs" folder of the project
		String inputGraphPath = args[0];
		String outputGraphPath = args[1];
		// find srcNo in readMe.txt file for each dataset
		Integer srcNo = Integer.parseInt(args[2]);
		new ClusteringExample().execute(method, inputGraphPath, outputGraphPath, srcNo);
	}

	public void execute(ClusteringMethods method, String srcFolder, String outFolder, Integer srcNo) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		GradoopFlinkConfig grad_config = GradoopFlinkConfig.createConfig((ExecutionEnvironment) env);

		boolean isEdgeBirection = false;
		ClusteringOutputType clusteringOutputType = ClusteringOutputType.GRAPH_COLLECTION;

		/***************************
		 * Load Similarity Graph
		 ********************************/
		JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", grad_config);
		LogicalGraph input = dataSource.getLogicalGraph();
		/***************************
		 * Clustering
		 ********************************/
		LogicalGraph resultGraph = null;
		LogicalGraph test = input;

		switch (method) {
		case CONCON:// concom
			resultGraph = test.callForGraph(new ConnectedComponents());
			break;

		case CLIP: // CLIP
			// the constructor of CLIPConfig sets the config parameters to some default values. They can be changed using setter methods
			CLIPConfig clipConfig = new CLIPConfig();
			clipConfig.setSourceNo(srcNo);
			resultGraph = test.callForGraph(new CLIP(clipConfig, clusteringOutputType));
			break;
		}
		/***************************
		 * Write Output Graph
		 ********************************/
//
		DateFormat dateFormat = new SimpleDateFormat("MMdd_HHmm");
		Date date = new Date();
		String timestamp = dateFormat.format(date);
		resultGraph.writeTo(new JSONDataSink(outFolder + timestamp + "/graphHeads.json", outFolder + timestamp + "/vertices.json", outFolder + timestamp + "/edges.json", grad_config));
		env.setParallelism(1);
		env.execute();

		/***************************
		 * Compute Output Precision, Recall, FMeasure
		 ********************************/
		Boolean hasOverlap = false;

		FileWriter fw = new FileWriter(outFolder+"quality.csv",true);
		fw.append("Pre,Rec,FM\n");
		ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures(resultGraph, "recId",  hasOverlap);
		fw.append(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+"\n");
		fw.flush();
	}
}

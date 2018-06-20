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
		CONCON, CORRELATION_CLUSTERING, CENTER, MERGE_CENTER, STAR1, STAR2, CLIP
	};

	public static void main(String args[]) throws Exception {
		// Clustering method can be chosen from the above list
		ClusteringMethods method = ClusteringMethods.CORRELATION_CLUSTERING;
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
		case CORRELATION_CLUSTERING: // CC
			resultGraph = test.callForGraph(new CorrelationClustering(isEdgeBirection, clusteringOutputType));
			break;
		case CENTER: // Center
			resultGraph = test.callForGraph(new Center(1, isEdgeBirection, clusteringOutputType));
			break;
		case MERGE_CENTER: // MC
			resultGraph = test.callForGraph(new MergeCenter(1, 0.0, isEdgeBirection, clusteringOutputType));
			break;
		case STAR1: // Star1
			resultGraph = test.callForGraph(new Star(1, 1, isEdgeBirection, clusteringOutputType));
			break;
		case STAR2: // Star2
			resultGraph = test.callForGraph(new Star(1, 2, isEdgeBirection, clusteringOutputType));
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
		if (method.equals(ClusteringMethods.STAR1) || method.equals(ClusteringMethods.STAR1))
			hasOverlap = true;
		FileWriter fw = new FileWriter(outFolder+"quality.csv",true);
		fw.append("Pre,Rec,FM\n");
		ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures(resultGraph, "recId",  hasOverlap);
		fw.append(eval.computePrecision()+","+eval.computeRecall()+","+eval.computeFM()+"\n");
		fw.flush();
	}
}

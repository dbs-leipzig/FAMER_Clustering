package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ERClustering;

import java.io.Serializable;

/**
 * The Structure of Message used in Center algorithm
 */
public class CenterMessage implements Serializable{

    private Long clusterId;
    private Double simDegree;
    private String additionalInfo;
    private String clusterIds;

    public  CenterMessage(Long ci, Double sd) {
        clusterId = ci;
        simDegree = sd;
        additionalInfo = "";
        clusterIds = "";
    }
    public Long getClusterId(){
        return clusterId;
    }
    public Double getSimDegree(){
        return simDegree;
    }
    public String getAdditionalInfo(){
        return additionalInfo;
    }
    public String getClusterIds(){
        return clusterIds;
    }


    public void setClusterId(Long newClusterId){
        clusterId = newClusterId;
    }
    public void setSimDegree(Double newSimDegree){
        simDegree = newSimDegree;
    }
    public void setAdditionalInfo(String addInfo){
        additionalInfo = addInfo;
    }
    public void setClusterIds(String cids){
        clusterIds = cids;
    }

}

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP;

import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.phase2.Phase2Method;

/**
 * CLIP Algorithm input parameters
 */
public class CLIPConfig {
    private Double delta;
    private Integer sourceNo;
    private Phase2Method phase2Method;
    private boolean removeSrcConsistentVertices;
    private Double simValueCoef;
    private Double degreeCoef;
    private Double strengthCoef;

    public CLIPConfig (){
        delta = 0d;
        phase2Method = Phase2Method.SEQUENTIAL_GROUPREDUCE;
        removeSrcConsistentVertices = false;
        simValueCoef = 0.5;
        degreeCoef = 0.2;
        strengthCoef = 0.3;
    }

    public void setDelta(Double delta) {
        this.delta = delta;
    }

    public void setStrengthCoef(Double strengthCoef) {
        this.strengthCoef = strengthCoef;
    }

    public void setDegreeCoef(Double degreeCoef) {
        this.degreeCoef = degreeCoef;
    }

    public void setSimValueCoef(Double simValueCoef) {
        this.simValueCoef = simValueCoef;
    }

    public void setRemoveSrcConsistentVertices(boolean removeSrcConsistentVertices) {
        this.removeSrcConsistentVertices = removeSrcConsistentVertices;
    }

    public void setPhase2Method(Phase2Method phase2Method) {
        this.phase2Method = phase2Method;
    }

    public void setSourceNo(Integer sourceNo) {
        this.sourceNo = sourceNo;
    }

    public Double getDelta() {

        return delta;
    }

    public Integer getSourceNo() {
        return sourceNo;
    }

    public Phase2Method getPhase2Method() {
        return phase2Method;
    }

    public boolean isRemoveSrcConsistentVertices() {
        return removeSrcConsistentVertices;
    }

    public Double getSimValueCoef() {
        return simValueCoef;
    }

    public Double getDegreeCoef() {
        return degreeCoef;
    }

    public Double getStrengthCoef() {
        return strengthCoef;
    }
}

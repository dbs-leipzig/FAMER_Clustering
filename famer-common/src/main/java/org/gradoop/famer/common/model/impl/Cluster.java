package org.gradoop.famer.common.model.impl;


import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class Cluster {
    private Collection<Vertex> vertices;
    private Collection<Edge> intraLinks;
    private Collection<Edge> interLinks;
    private String clusterId;
    public boolean isPerfect;
    private boolean isCompletePerfect;
    private boolean isPerfectIsolated;
    private String componentId;

    public Cluster (Cluster c){
        vertices = c.getVertices();
        intraLinks = c.getIntraLinks();
        interLinks = c.getInterLinks();
        clusterId = c.getClusterId();
        isPerfect = c.getIsPerfect();
        isCompletePerfect = c.getIsCompletePerfect();
        componentId = c.getComponentId();
        isPerfectIsolated = c.getIsPerfectIsolated();
    }
    public Cluster(Collection<Vertex> Vertices){
        vertices = Vertices;
        intraLinks = new ArrayList<>();
        interLinks = new ArrayList<>();
        clusterId = componentId = "";
        isPerfect = isCompletePerfect = isPerfectIsolated = false;
    }
    public Cluster(Collection<Vertex> Vertices, String inputClusterId){
        vertices = Vertices;
        intraLinks = new ArrayList<>();
        interLinks = new ArrayList<>();
        componentId = "";
        clusterId = inputClusterId;
        isPerfect = isCompletePerfect = isPerfectIsolated = false;
    }
    public void setClusterId(String newClusterId){
        clusterId = newClusterId;
    }


    public Cluster(Collection<Vertex> Vertices,
                   Collection<Edge> IntraLinks,
                   Collection<Edge> InterLinks,
                   String ClusterId,
                   String ComponentId) {
        vertices = Vertices;
        intraLinks = IntraLinks;
        interLinks = InterLinks;
        clusterId = ClusterId;
        isPerfect = isCompletePerfect = isPerfectIsolated = false;
        componentId = ComponentId;
    }

    public Cluster() {
        vertices = new ArrayList<>();
        intraLinks = new ArrayList<>();
        interLinks = new ArrayList<>();
        clusterId = componentId = "";
        isPerfect = isCompletePerfect = isPerfectIsolated = false;
    }
    public String getClusterId(){return clusterId;}
    public Collection<Vertex> getVertices(){return vertices;}
    public Collection<Edge> getIntraLinks(){return intraLinks;}
    public Collection<Edge> getInterLinks(){return interLinks;}
    public boolean getIsPerfect(){return isPerfect;}
    public boolean getIsCompletePerfect(){return isCompletePerfect;}
    public boolean getIsPerfectIsolated(){return isPerfectIsolated;}




    // which factors? No. of links or degree of them, or a combination?
    // should we consider the non-determinstic vertices in computing association degree?

    //Version1 : all vertices are considered
    public Double getAssociationDegree (String VertexGradoopId){
        Double sum = 0d;
        Integer count = 0;
        for (Edge e: intraLinks){
            if (e.getTargetId().toString().equals(VertexGradoopId) || e.getSourceId().toString().equals(VertexGradoopId)){
                count ++;
                sum+= Double.parseDouble(e.getPropertyValue("value").toString());
            }
        }
        if (count == 0)
            return 0d;
        return sum/count;
    }

    //Version2: only deterministic vertices are considered
    public Double getAssociationDegree2 (String VertexGradoopId){
        Double sum = 0d;
        Integer count = 0;
        Collection<String> forbiddenVerticesGradoopIds = new ArrayList<>();
        for (Vertex v: vertices){
            if (v.getPropertyValue("ClusterId").toString().contains(","))
                forbiddenVerticesGradoopIds.add(v.getId().toString());
        }
        for (Edge e: intraLinks){
            if ((e.getTargetId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getSourceId().toString()))
                    || (e.getSourceId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getTargetId().toString()))){
                count ++;
                sum+= Double.parseDouble(e.getPropertyValue("value").toString());
            }
        }
        if (count == 0)
            return 0d;
        return sum/count;
    }

    //Version3: deterministic vertices are considered for computing ave association-degree and the number of vertices with overlaps are counted
    public Tuple2<Double, Integer> getAssociationDegree3 (String VertexGradoopId){
        Double sum = 0d;
        Integer count = 0;
        Collection<String> forbiddenVerticesGradoopIds = new ArrayList<>();
        for (Vertex v: vertices){
            if (v.getPropertyValue("ClusterId").toString().contains(","))
                forbiddenVerticesGradoopIds.add(v.getId().toString());
        }
        for (Edge e: intraLinks){
            if ((e.getTargetId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getSourceId().toString()))
                    || (e.getSourceId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getTargetId().toString()))){
                count ++;
                sum+= Double.parseDouble(e.getPropertyValue("value").toString());
            }
        }
        int count2 = 0;
        for (Edge e: intraLinks){
            if ((e.getTargetId().toString().equals(VertexGradoopId) && forbiddenVerticesGradoopIds.contains(e.getSourceId().toString()))
                    || (e.getSourceId().toString().equals(VertexGradoopId) && forbiddenVerticesGradoopIds.contains(e.getTargetId().toString()))){
                count2 ++;
            }
        }
        if (count == 0)
            return Tuple2.of(0d, count2);
        return Tuple2.of(sum/count, count2);
    }

    //Version4: deterministic vertices + vertices with same level of overlap
    public Double getAssociationDegree4 (String VertexGradoopId) {
        Double sum = 0d;
        Integer count = 0;
        Collection<String> forbiddenVerticesGradoopIds = new ArrayList<>();
        Integer allowedOverlapLength = 0;
        for (Vertex v : vertices) {
            if (v.getId().toString().equals(VertexGradoopId))
                allowedOverlapLength = v.getPropertyValue("ClusterId").toString().split(",").length;
        }
        for (Vertex v : vertices) {
            Integer overlapLength = v.getPropertyValue("ClusterId").toString().split(",").length;
            if (overlapLength > allowedOverlapLength) {
                if (!forbiddenVerticesGradoopIds.contains(v.getId().toString()))
                    forbiddenVerticesGradoopIds.add(v.getId().toString());
            }
        }
        for (Edge e : intraLinks) {
            if ((e.getTargetId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getSourceId().toString()))
                    || (e.getSourceId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getTargetId().toString()))) {
                count++;
                sum += Double.parseDouble(e.getPropertyValue("value").toString());
            }
        }

//        if (count == 0)
//            return 0d;
        Integer clusterSize = vertices.size()-(forbiddenVerticesGradoopIds.size()+1);
        return sum / clusterSize;
    }
    public Double getAssociationDegree40 (String VertexGradoopId) {
        Double sum = 0d;
        Integer count = 0;
        Collection<String> forbiddenVerticesGradoopIds = new ArrayList<>();

        for (Vertex v : vertices) {
            Integer overlapLength = v.getPropertyValue("ClusterId").toString().split(",").length;
            if (overlapLength > 1) {
                if (!forbiddenVerticesGradoopIds.contains(v.getId().toString()))
                    forbiddenVerticesGradoopIds.add(v.getId().toString());
            }
        }
        for (Edge e : intraLinks) {
            if ((e.getTargetId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getSourceId().toString()))
                    || (e.getSourceId().toString().equals(VertexGradoopId) && !forbiddenVerticesGradoopIds.contains(e.getTargetId().toString()))) {
                count++;
                sum += Double.parseDouble(e.getPropertyValue("value").toString());
            }
        }

//        if (count == 0)
//            return 0d;
        Integer clusterSize = vertices.size()-(forbiddenVerticesGradoopIds.size()+1);
        return sum / clusterSize;
    }

    //  only deterministics
    public Boolean isMergingPossible (Cluster cluster){
        Collection<Vertex> clusterVertices = cluster.getVertices();
        Set<String> clusterSources = new HashSet<String>();
        Set<String> sources = new HashSet<String>();
        for (Vertex v: clusterVertices)
            if (!v.getPropertyValue("ClusterId").toString().contains(","))
                clusterSources.add(v.getPropertyValue("srcId").toString());
        for (Vertex v: vertices)
            if (!v.getPropertyValue("ClusterId").toString().contains(","))
                sources.add(v.getPropertyValue("srcId").toString());
        clusterSources.retainAll(sources);
        if (clusterSources.size() == 0)
            return true;
        return false;
    }

    //  all
    public Boolean isMergingPossible2 (Cluster cluster){
        Collection<Vertex> clusterVertices = cluster.getVertices();
        Set<String> clusterSources = new HashSet<String>();
        Set<String> sources = new HashSet<String>();
        for (Vertex v: clusterVertices)
                clusterSources.add(v.getPropertyValue("srcId").toString());
        for (Vertex v: vertices)
                sources.add(v.getPropertyValue("srcId").toString());
        clusterSources.retainAll(sources);
        if (clusterSources.size() <= 1)
            return true;
        return false;
    }
    public Boolean hasOverlap (String ExceptionVertexId){
        for (Vertex v: vertices){
            if (!v.getId().toString().equals(ExceptionVertexId) && v.getPropertyValue("ClusterId").toString().contains(","))
                return true;
        }
        return false;
    }
    public Collection<String> getSources(){
        Collection<String> sources = new ArrayList<>();
        for (Vertex v: vertices){
            sources.add(v.getPropertyValue("srcId").toString());
        }
        return sources;
    }
    public Boolean isConsistent (){
        Collection<String> sources = new ArrayList<>();
        for (Vertex v: vertices){
            String src = v.getPropertyValue("srcId").toString();
            if (sources.contains(src))
                return false;
            sources.add(src);
        }
        return true;
    }
    public boolean isDifferent (Cluster c){
        if (c.getVertices().size() != vertices.size())
            return true;
        Collection<String> ids = new ArrayList<>();
        Collection<String> otherClusterIds = new ArrayList<>();
        for (Vertex v: vertices)
            ids.add(v.getId().toString());
        for (Vertex v: c.getVertices())
            otherClusterIds.add(v.getId().toString());
        for (String id: ids){
            if (!otherClusterIds.contains(id))
                return true;
        }
        return false;
    }
    public void removeFromInterLinks (Collection<Edge> Edges) {
        for (Edge e: Edges)
            interLinks.remove(e);
    }
    public void add2InterLinks (Collection<Edge> Edges) {
        for (Edge e: Edges)
            interLinks.add(e);
    }
    public void add2InterLinks (Edge Edge) {
            interLinks.add(Edge);
    }
    public void setInterLinks (Collection<Edge> edges){
        interLinks = edges;
    }
    public void removeFromVertices(Vertex vertex){
        vertices.remove(vertex);
    }
    public void removeFromVertices(GradoopId id){
        for (Vertex v:vertices) {
            if (v.getId().equals(id)){
                vertices.remove(v);
                break;
            }
        }
    }
    public Vertex getVertex(GradoopId id){
        for (Vertex v:vertices) {
            if (v.getId().equals(id)){
                return v;
            }
        }
        return null;
    }
    public void addToVertices(Vertex vertex){
        vertices.add(vertex);
    }
    public Boolean hasOverlap(){
        for (Vertex v:vertices){
            if (v.getPropertyValue("ClusterId").toString().contains(",")){
                return true;
            }
        }
        return false;
    }




    public void setIsCompletePerfect(Integer SourceNo){
        isCompletePerfect =  false;
        isPerfect = true;
        Collection<String> srcs = new ArrayList<>();
        for (Vertex v: vertices){
            String src = v.getPropertyValue("srcId").toString();
            if (!srcs.contains(src))
                srcs.add(src);
            else {
                isPerfect = false;
                break;
            }
        }
        if (isPerfect && srcs.size() == SourceNo) {
            isCompletePerfect = true;
        }
        Boolean isIsolated = false;
        if (interLinks.size() == 0)
            isIsolated = true;
        if (isPerfect && isIsolated && !isCompletePerfect)
            isPerfectIsolated = true;
    }

    public boolean isCompatible(Cluster other){
        Collection<String> types = new ArrayList<>();
        for (Vertex v: vertices){
            types.add(v.getPropertyValue("srcId").toString());
        }
        for (Vertex v: other.getVertices()){
            if (types.contains(v.getPropertyValue("srcId").toString()))
                return false;
        }
        return true;
    }
    public void setComponentId (String ComponentId){
        componentId = ComponentId;
    }
    public String getComponentId(){return componentId;}
    public Collection<Vertex> getRelatedVertices(GradoopId vertexId){
        Collection<Vertex> relatedVertices = new ArrayList<>();
        for (Edge e: intraLinks){
            if (e.getSourceId().equals(vertexId))
                relatedVertices.add(getVertex(e.getTargetId()));
            else if (e.getTargetId().equals(vertexId))
                relatedVertices.add(getVertex(e.getSourceId()));
        }
        return relatedVertices;
    }
    public void printCluster(){
        System.out.println(clusterId);
        for(Vertex v:vertices){
            System.out.print(" "+v.getPropertyValue("recId"));
        }
        System.out.println();
        System.out.print("intra: ------------------------------");
        for(Edge e:intraLinks){
            System.out.println(e.getId()+", "+e.getPropertyValue("value"));
        }
        System.out.print("inter: ------------------------------");
        for(Edge e:interLinks){
            System.out.println(e.getId()+", "+e.getPropertyValue("value"));
        }
        System.out.print("*******************************");
    }

}





























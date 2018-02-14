package com.kartik.rapid.pojo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is the data object class. The performQuickClustering sets data into its 2 objects, clusterSet & parentIdClusterMapping.
 * clusterSet - Has grouping of all the parentIds which share the same cluster. (clusterID -- all parentIds belonging in that cluster)
 * parentIdClusterMapping - Has the one-to-one parentId--clusterID mapping for all the parentId keys present in the input
 * @author : Kartik Iyer
 */
public class ClusteringResult
{
	Map<Integer, Set<Long>>	clusterSet			= new HashMap<>();
	Map<Long, Integer>		parentIdClusterMapping	= new HashMap<>();

	/**
	 * Gets the cluster set.
	 * Has grouping of all the parentIds which share the same cluster. (clusterID -- all parentIds belonging in that cluster)
	 *
	 * @author : Kartik Iyer
	 * @return the cluster set
	 */
	public Map<Integer, Set<Long>> getClusterSet()
	{
		return clusterSet;
	}

	/**
	 * Sets the cluster set.
	 * Has grouping of all the parentIds which share the same cluster. (clusterID -- all parentIds belonging in that cluster)
	 *
	 * @author Kartik Iyer
	 * @param clusterSet the cluster set
	 */
	public void setClusterSet(Map<Integer, Set<Long>> clusterSet)
	{
		this.clusterSet = clusterSet;
	}

	/**
	 * Gets the parent id cluster mapping.
	 * Has the one-to-one parentId--clusterID mapping for all the parentId keys present in the input
	 *
	 * @author : Kartik Iyer
	 * @return the parent id cluster mapping
	 */
	public Map<Long, Integer> getParentIdClusterMapping()
	{
		return parentIdClusterMapping;
	}

	/**
	 * Sets the parent id cluster mapping.
	 * Has the one-to-one parentId--clusterID mapping for all the parentId keys present in the input
	 *
	 * @author Kartik Iyer
	 * @param parentIdClusterMapping the parent id cluster mapping
	 */
	public void setParentIdClusterMapping(Map<Long, Integer> parentIdClusterMapping)
	{
		this.parentIdClusterMapping = parentIdClusterMapping;
	}


}

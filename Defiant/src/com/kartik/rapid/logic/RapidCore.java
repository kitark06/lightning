package com.kartik.rapid.logic;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.net.aso.d;

import org.apache.log4j.Logger;

import com.kartik.rapid.pojo.ClusteringResult;


/**
 * RapidCore -
 * The core class of RapidClusterer. Methods inside this class are responsible for accelerating the clustering speed.
 * Each and every decision taken in this class was to increase the execution speed.
 *
 * @param <T> This class accepting a generic type parameter for the objects which needs to be clustered.
 */
public class RapidCore<T>
{
	private Integer			nextSimpleKey		= 0;

	private Integer[]			mapping;
	private Long[]				invertedMappingArray;
	private Map<Long, Integer>	simplifiedMapping	= new HashMap<>();

	static final Logger			log				= Logger.getLogger(RapidCore.class);

	/**
	 * The main processing method which is responsible for performing the actual rapid clustering.
	 * Takes a Map of generic objects and set of keys. The keys are not important as Clusters are totally based on the value.
	 *
	 * @author Kartik Iyer.
	 * @param dataMap - The input map which has a generic key, and Set of long values. the keys are ignored during clustering.
	 * @param distinctIDCount - The distinct id count is NOT the bigram count. This is max count of all the distinct values possibly present in the input value part of the map.
	 * Check {@link InputTableDao} createDataMapFromTable for an example implementation.
	 * @return A ClusteringResult object with 2 fields via which the result can be extracted.
	 */
	public ClusteringResult performQuickClustering(Map<T, Set<Long>> dataMap, int distinctIDCount)
	{
		if(dataMap == null || dataMap.size() < 1 )
			throw new RuntimeException("Input has no data.");

		long algoStartTime = System.currentTimeMillis();

		log.info("Starting Rapid Clustering");
		log.info("Creating arrays with size of distinctIDCount :: " + distinctIDCount);
		Integer[] bigArray = new Integer[distinctIDCount];
		mapping = new Integer[distinctIDCount];
		invertedMappingArray = new Long[distinctIDCount];

		log.debug("Populating the mapping entries where initially each entry points to itself");
		// populate the mapping entries .. at start, each entry points to itself.. this will change as prog executes..
		for (int i = 0; i < mapping.length; i++)
			mapping[i] = i;

		int nextClusterID = 0;
		boolean generateNewId = false;
		int size = dataMap.keySet().size(); // 3903,072 vs 8678,388

		log.info("Starting processing of " + size + " biGrams ");
		int mainLoopCounter = 0;


		for (T biGram : dataMap.keySet())
		{
			// BiGram cant be null or a 0 length string .. hence ignore such entries
			if (biGram != null /*&& biGram.isEmpty() == false*/)
			{
				long startTime = System.currentTimeMillis();

				// .. NEW CLUSTER ID WILL BE GENERATED ie it will be INCREMENTED only if ClusterID was assigned in below steps
				if (generateNewId)
					nextClusterID++;

				Set<Long> currentSet = dataMap.get(biGram);

				// Set of integers in currentSet cant be 0 , ie currentSet cant be empty ..  ignore such entries
				if (currentSet != null && currentSet.size() > 0)
				{
					// Single key entry encountered
					if (currentSet.size() == 1)
					{
						int singleElement = getSimplifiedMapping(currentSet.iterator().next());
						log.trace(currentSet.iterator().next() + " :: " + singleElement);

						// encountered for the first time .. thus make an entry
						if (bigArray[singleElement] == null)
						{
							bigArray[singleElement] = nextClusterID;
							generateNewId = true;
						}

						// Single key set entry repeated .. which already exists so do nothing
						// if (bigArray[singleElement] != null)
						else
							generateNewId = false;
					}
					else
					{
						// Assume clusters wont be formed, this will only change if null entry in bigArray is encountered, in which case, generateNewId = true
						generateNewId = false;
						// used to keep track of changes and to implement the PREV condition
						int prevKeyCluster = -1;

						log.trace("currentSet is "+currentSet);

						// ----------------------------- START OF A CLUSTERED IP SET -----------------------------
						for (Long key : currentSet)
						{
							int simpleKey = getSimplifiedMapping(key);

							log.trace("Original key was "+key + " :: " + " simpleKey is " + simpleKey );

							// KEY doesnt belong to a cluster.. so assign it a new cluster
							if (bigArray[simpleKey] == null)
							{
								log.trace("SimpleKey " + simpleKey + " doesnt belong to a cluster.. so assign it a new cluster");
								if (prevKeyCluster != -1 && prevKeyCluster != nextClusterID)
								{
									if (nextClusterID < prevKeyCluster)
									{
										log.fatal("Fatal error in clusterID generation. NextCLusterID < PreviousClusterID is not possible");
										throw new RuntimeException();
									}
									if (compressAndGetParent(prevKeyCluster) != nextClusterID)
									{
										log.trace(" Setting mapping[" + compressAndGetParent(prevKeyCluster) + "] as " + nextClusterID);
										mapping[compressAndGetParent(prevKeyCluster)] = nextClusterID;
									}
								}

								// for a valid condition
								bigArray[simpleKey] = nextClusterID;
								// current clusterID will be prev one for next iteration.
								prevKeyCluster = nextClusterID;
								generateNewId = true;
							}

							// KEY already belongs to a cluster
							else
							{
								int assignedClusterId = bigArray[simpleKey];

								log.trace("SimpleKey " + simpleKey + " already belongs to a cluster");
								// first entry in the set wont have any previous value to compare with
								if (prevKeyCluster == -1)
									prevKeyCluster = assignedClusterId;

								// there is gonna be new cluster formation !!!
								// make prev cluster point to new cluster ie make set lesserCluster to greaterCluster for maintaining consistency
								if (prevKeyCluster != -1 && prevKeyCluster != assignedClusterId)
								{
									int lesserCluster, greaterCluster;
									int parentCluster1 = compressAndGetParent(prevKeyCluster);
									int parentCluster2 = compressAndGetParent(assignedClusterId);

									if (parentCluster1 != parentCluster2)
									{
										if (parentCluster1 < parentCluster2)
										{
											lesserCluster = parentCluster1;
											greaterCluster = parentCluster2;
										}
										else
										{
											lesserCluster = parentCluster2;
											greaterCluster = parentCluster1;
										}

										log.trace("Making lesserCluster cluster point to greaterCluster");
										log.trace("mapping[" + lesserCluster + "] = " + greaterCluster);
										mapping[lesserCluster] = greaterCluster;
									}
								}

								// current key will be prevKey in next iteration.
								if (prevKeyCluster != assignedClusterId)
									prevKeyCluster = assignedClusterId;
							}
						}
						// ----------------------------- END OF THE CLUSTERED IP SET -----------------------------
					}

				}

				// counter used for logging purposes.
				mainLoopCounter++;
				if (mainLoopCounter % 10000 == 0)
					log.info("Bigrams Processed --> " + mainLoopCounter + " :: Total -->  " + size + " :: MILIseconds Taken " + ((System.currentTimeMillis() - startTime)));
//				log.info("Took " + ((System.currentTimeMillis() - startTime)) + " MILIseconds to process --> " + mainLoopCounter + " outta " + size);

			}
		}

		log.info("BiGram processing completed successfully");
		log.info("Total bigrams processed :: " + size);

		log.info("Performing FINAL Path Compression for faster access");
		// NOTE TO SELF :- ONLY COMPACT ENTRIES EQUAL TO THE CLUSTERS GENERATED
		for (int i = 0; i <= nextClusterID; i++)
			mapping[i] = compressAndGetParent(i);

		log.info("Coalescing the output");

		// at this point each index of bigArray has a value which corresponds to its mapping.
		// so iterate thru all of them, get their clusterID ..
		// create a map of sets
		// keys is the clusterID and values is all the indices of bigArray which have 'clusterID' as value..

		log.info("Generating 2 result maps .. clusterSet & parentIdClusterMapping");
		Map<Integer, Set<Long>> clusterSet = new HashMap<>();
		Map<Long, Integer> parentIdClusterMapping = new HashMap<>();

		for (int simpleKey = 0; simpleKey < bigArray.length; simpleKey++)
		{
			Integer clusterID = mapping[bigArray[simpleKey]];
			Long originalKey = invertedMappingArray[simpleKey];

			parentIdClusterMapping.put(originalKey, clusterID);

			if (clusterSet.get(clusterID) == null)
			{
				HashSet<Long> hs = new HashSet<>();
				hs.add(originalKey);
				clusterSet.put(clusterID, hs);
			}
			else
			{
				clusterSet.get(clusterID).add(originalKey);
			}
		}

		ClusteringResult result = new ClusteringResult();
		result.setClusterSet(clusterSet);
		result.setParentIdClusterMapping(parentIdClusterMapping);

		log.info(" -- FINALE -- ");
		log.info("Total time required for Rapid Clustering is " + ((System.currentTimeMillis()-algoStartTime)/1000) + " seconds " );

		return result;
	}


	/**
	 * Gets the simplified mapping. Converts user input of non consecutive long keys, to a ordered and consecutive int values.
	 * This uniformity is important as an array of all these values is maintained, which gives the algo the reqd speed.
	 * Since its an array, there should not be any missing values as it leads to space wastage of the indices of the array.
	 *
	 * eg :: input 1 , 2 , 50 , 100 is converted to 0 , 1 , 2 , 3
	 *
	 * @author Kartik Iyer
	 *
	 * @param key the key
	 * @return the simplified mapping
	 */
	private Integer getSimplifiedMapping(Long key)
	{
		int simpleKey;
		if (simplifiedMapping.get(key) != null)
		{
			simpleKey = simplifiedMapping.get(key);
			log.trace("Key " + key + " is has the value " + simpleKey);
		}
		else
		{
			log.trace("Key " + key + " encountered for first time.. creating an entry in map and mapping it to " + nextSimpleKey);
			simplifiedMapping.put(key, nextSimpleKey);
			invertedMappingArray[nextSimpleKey] = key;
			simpleKey = nextSimpleKey;
			nextSimpleKey++;
		}
		return simpleKey;
	}

	/**
	 * Finds the parent of the clusterID and also performs path Compression for increasing speed.
	 * Makes recursive calls to the getParentRecursive method to get the desired info.
	 *
	 * @author Kartik Iyer
	 * @param i the key of which parent is to be found
	 * @return the parent of the key i
	 */
	private int compressAndGetParent(int i)
	{
		int parent = getParentRecursive(i);
		mapping[i] = parent;
		log.trace("Returning parent " + parent + " for key " + i);
		return parent;
	}

	/**
	 * Recursively gets the parent.
	 *
	 * @author : Kartik Iyer
	 *
	 * @param i the i
	 * @return the recursive parent.
	 */
	private int getParentRecursive(int i)
	{
		if (mapping[i] == i)
			return i;
		else
			return compressAndGetParent(mapping[i]);
	}
}

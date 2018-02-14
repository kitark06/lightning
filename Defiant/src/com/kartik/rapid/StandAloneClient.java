package com.kartik.rapid;


import static com.kartik.rapid.io.RapidClusteringProperties.COLUMN_DELIM;
import static com.kartik.rapid.io.RapidClusteringProperties.CONNECTION_STRING;
import static com.kartik.rapid.io.RapidClusteringProperties.DATA_DELIM;
import static com.kartik.rapid.io.RapidClusteringProperties.DESCRIPTION_COLUMNS;
import static com.kartik.rapid.io.RapidClusteringProperties.ENDNODE_STR;
import static com.kartik.rapid.io.RapidClusteringProperties.ID_COLUMN;
import static com.kartik.rapid.io.RapidClusteringProperties.TABLE_NAME;
import static com.kartik.rapid.io.RapidClusteringProperties.getProperty;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.kartik.rapid.dao.InputTableDao;


public class StandAloneClient
{

	static final Logger	log	= Logger.getLogger(StandAloneClient.class);

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{
		long startTime = System.currentTimeMillis();

		// the getProperty and its string constants belong to com.dummy.rapid.io.RapidClusteringProperties
		String dataDelim = getProperty(DATA_DELIM);
		String columnDelim = getProperty(COLUMN_DELIM);
		String endnodeStr = getProperty(ENDNODE_STR);
		String tableName = getProperty(TABLE_NAME);
		String idColumn = getProperty(ID_COLUMN);
		String columns = getProperty(DESCRIPTION_COLUMNS);
		String connectionString = getProperty(CONNECTION_STRING);

		log.info("dataDelim " + dataDelim);
		log.info("columnDelim "+columnDelim);
		log.info("endnodeStr "+endnodeStr);

		log.info("tableName  "+tableName);
		log.info("idColumn  " + idColumn);
		log.info("columns " + columns);
		log.info("connectionString "+ connectionString);

		new StandAloneClient().startProcess(tableName, idColumn, columns, columnDelim, connectionString, dataDelim, endnodeStr);
		log.info("Total Time Taken is " + (System.currentTimeMillis() - startTime) / 1000 + " seconds ");
	}

	private void startProcess(String tableName, String idColumn, String columns, String columnDelim, String connectionString, String dataDelim, String endnodeStr)
		throws ClassNotFoundException, SQLException, IOException
	{
		log.debug("Calling createDataMapFromTable ");
		InputTableDao dao = new InputTableDao(tableName, idColumn, columns, columnDelim, connectionString, dataDelim, endnodeStr);
		Map<Object, Set<Long>> map = dao.createDataMapFromTable();
		
		//Saving of object in a file
          FileOutputStream file = new FileOutputStream("C:\\Users\\kartik.iyer\\Desktop\\mapCompressed.gz");
          GZIPOutputStream gz = new GZIPOutputStream(file);
          ObjectOutputStream out = new ObjectOutputStream(gz);
           
          // Method for serialization of object
          out.writeObject(map);
           
          out.close();
          file.close();

	/*	log.debug("Starting RapidCluster ");
		RapidCore cluster = new RapidCore();
		ClusteringResult result = cluster.performQuickClustering(map, dao.getDistinctIDCount());
		Map<Integer, Set<Long>> clusterSet = result.getClusterSet();

		log.debug("Starting BackUpdate of ClusterIDs");
		dao.performBackUpdateToDatabase(clusterSet);*/
	}
}

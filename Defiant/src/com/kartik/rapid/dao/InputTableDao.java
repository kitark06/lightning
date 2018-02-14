package com.kartik.rapid.dao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.kartik.rapid.utility.BiGramUtility;


/**
 * The InputTableDao class performs JDBC with the data if RapidClusterer is to be used as a StandAloneMode app.
 *
 * @author Kartik Iyer
 */
public class InputTableDao
{
	static final Logger			log				= Logger.getLogger(InputTableDao.class);
	private static final String	CLUSTERID_COLUMN_NAME	= "cluid";


	private String				tableName;
	private String				idColumn;
	private String				columns;
	private String				columnDelim;
	private String				connectionString;
	private BiGramUtility		biGramGenerator;
	private Integer			distinctIDCount;


	/**
	 * Gets the distinct id count.
	 *
	 * @author : Kartik Iyer
	 * @return the distinct id count
	 */
	public Integer getDistinctIDCount()
	{
		return distinctIDCount;
	}

	/**
	 * Instantiates a new input table dao.
	 *
	 * @param tableName - the table name
	 * @param idColumn - the column with primaryKey
	 * @param columns - the other columns in CSV format. DO NOT repeat the idColumn in this set.
	 * @param columnDelim - the column delimiter
	 * @param connectionString - the connection string
	 * @param dataDelim - the data delimiter
	 * @param endnodeStr - the String which is used to buffer / pad the single token elements to generate a combination.
	 */
	public InputTableDao(String tableName, String idColumn, String columns, String columnDelim, String connectionString, String dataDelim, String endnodeStr)
	{
		super();
		this.tableName = tableName;
		this.idColumn = idColumn;
		this.columns = columns;
		this.columnDelim = columnDelim;
		this.connectionString = connectionString;
		biGramGenerator = new BiGramUtility(dataDelim, endnodeStr);
	}


	/**
	 * Creates the data map from the input table.
	 *
	 * @author Kartik Iyer
	 * @return the map
	 * @throws SQLException the SQL exception
	 * @throws ClassNotFoundException the class not found exception
	 */
	public Map<Object, Set<Long>> createDataMapFromTable() throws SQLException, ClassNotFoundException
	{

		log.info("Initiating createDataMapFromTable");

		Class.forName("oracle.jdbc.driver.OracleDriver");

		Integer id = new Integer(0);
		Set<Long> longSet;
		Map<Object, Set<Long>> dataMap = new HashMap<Object, Set<Long>>();

		log.info("Conn established");

		long totalRowCount = getTotalRowCount();

		// Done to prevent data inconsistency which might happen due to data present from previous run.
		dropAndCreateClusterIDColumn();

		try (
			Connection conn = DriverManager.getConnection(connectionString);
			Statement stmt = conn.createStatement();)
		{
			String selectQuery = "select " + idColumn + "," + columns + " from " + tableName;
			log.debug(selectQuery);
			try (
				ResultSet rs = stmt.executeQuery(selectQuery);)
			{

				int tableSiphonCounter = 0;
				while (rs.next())
				{
					StringBuilder row = new StringBuilder();

					for (String columnLabel : columns.split(","))
					{
						row.append(rs.getString(columnLabel) + columnDelim);
					}
					row.deleteCharAt(row.length() - 1);

					boolean generateID = false;

					for (String columnData : row.toString().split("\\" + columnDelim))
					{
						if (columnData.equalsIgnoreCase("null") == false)
						{
							generateID = true;
							for (String biGram : biGramGenerator.generateBiGram(columnData))
							{
								if (dataMap.containsKey(biGram))
									longSet = dataMap.get(biGram);
								else
									longSet = new HashSet<Long>();

								longSet.add(Long.parseLong(rs.getString(idColumn)));
								dataMap.put(biGram, longSet);
							}
						}
					}

					// finished processing current row .. going for next one
					if (generateID == true)
						id++;

					// counter used for logging purposes.
					tableSiphonCounter++;
					if (tableSiphonCounter % 10000 == 0)
						log.info("Reading row --> " + tableSiphonCounter + " :: Total Rows " + totalRowCount);
				}
			}
		}

		distinctIDCount = id;
		return dataMap;
	}


	/**
	 * Performs back update of ClusterIDs wrt the ParentIDs in the database.
	 *
	 * @author Kartik Iyer
	 * @param clusterSet the cluster set
	 * @throws SQLException the SQL exception
	 */
	public void performBackUpdateToDatabase(Map<Integer, Set<Long>> clusterSet) throws SQLException
	{

		long totalRowCount = getTotalRowCount();
		log.info("Starting backupdating of " + totalRowCount + " rows ");
		try (
			Connection connAutoCommitOff = DriverManager.getConnection(connectionString);)
		{
			connAutoCommitOff.setAutoCommit(false);
			String backUpdateSql = "update " + tableName + " set "+CLUSTERID_COLUMN_NAME+"=? where " + idColumn + "=?";
			log.debug(backUpdateSql);

			try (
				PreparedStatement pstmt = connAutoCommitOff.prepareStatement(backUpdateSql);)
			{
				int batchCounter = 0;

				for (Integer cluID : clusterSet.keySet())
				{
					for (Long i : clusterSet.get(cluID))
					{
						Long primaryKey = i;
						pstmt.setString(1, cluID.toString());
						pstmt.setLong(2, primaryKey);
						pstmt.addBatch();
						batchCounter++;

						if (batchCounter % 5000 == 0)
						{
							pstmt.executeBatch();
							log.info("Executed batches --> " + batchCounter + " :: Total rows --> " + totalRowCount);
						}
					}
				}

				pstmt.executeBatch();
			}
			connAutoCommitOff.commit();
		}
		catch (SQLException e)
		{
			log.error("Batch Backupdate of clusterID failed. Hence reverting the run by dropping and then creating the column "+CLUSTERID_COLUMN_NAME,e);
			dropAndCreateClusterIDColumn();
			throw new SQLException("Batch Backupdate of clusterID failed",e);
		}
	}

	/**
	 * Drops and then creates the cluster id column.
	 *
	 * @author Kartik Iyer
	 * @throws SQLException the SQL exception
	 */
	private void dropAndCreateClusterIDColumn() throws SQLException
	{

		try (
			Connection conn = DriverManager.getConnection(connectionString);
			Statement stmt = conn.createStatement();)
		{
			log.info("Dropping & Creating column " + CLUSTERID_COLUMN_NAME + " if it exists");
			try
			{
				log.debug("Dropping column " + CLUSTERID_COLUMN_NAME + " if it exists");
				stmt.execute("Alter table " + tableName + " drop column " + CLUSTERID_COLUMN_NAME);
			}
			catch (Exception e)
			{
				log.warn(e.toString());
			}

			try
			{
				log.debug("Creating column " + CLUSTERID_COLUMN_NAME + " if it does not exists");
				stmt.execute("Alter table " + tableName + " add " + CLUSTERID_COLUMN_NAME + " varchar(50)");
			}
			catch (Exception e)
			{
				log.warn(e.toString());
			}
		}
	}

	/**
	 * Gets the total row count of the input table.
	 *
	 * @author : Kartik Iyer
	 * @return the total row count
	 * @throws SQLException the SQL exception
	 */
	private long getTotalRowCount() throws SQLException
	{
		long totalRowCount;

		String totalRowCountQuery = "select count(" + idColumn + ") from " + tableName;
		try (
			Connection conn = DriverManager.getConnection(connectionString);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(totalRowCountQuery))
		{
			rs.next();
			totalRowCount = rs.getLong(1);
		}

		log.info("Returning total rowcount " + totalRowCount);
		return totalRowCount;
	}

}

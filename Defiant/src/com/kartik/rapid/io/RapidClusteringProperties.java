package com.kartik.rapid.io;


import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;


public class RapidClusteringProperties
{

	static Properties			rcProperties		= new Properties();

	static final Logger			log				= Logger.getLogger(RapidClusteringProperties.class);

	public static final String	DATA_DELIM		= "dataDelim";
	public static final String	COLUMN_DELIM		= "columnDelim";
	public static final String	ENDNODE_STR		= "endnodeStr";
	public static final String	TABLE_NAME		= "tableName";
	public static final String	ID_COLUMN			= "idColumn";
	public static final String	DESCRIPTION_COLUMNS	= "descriptionColumns";
	public static final String	CONNECTION_STRING	= "connectionString";

	static
	{
		try
		{
			rcProperties.load(RapidClusteringProperties.class.getResourceAsStream("/RapidClustering.properties"));
		}
		catch (IOException e)
		{
			log.error(e.getMessage(), e);
		}
	}

	// public RapidClusteringProperties() throws IOException
	// {
	// this.rcProperties = new Properties();
	// rcProperties.load(this.getClass().getClassLoader().getResourceAsStream("RapidClustering.properties"));
	// }

	public static String getProperty(String key)
	{
		String value;
		value = rcProperties.getProperty(key);
		log.debug("Key " + key + " has value " + value);

		if (value == null || value.isEmpty())
			log.error("Key " + key + " is either misssing or has null value");

		return value;
	}
}

package com.kartik.rapid.utility;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class BiGramUtility.
 */
public class BiGramUtility
{
	private String				dataDelim;
	private String				endnodeStr;

	/**
	 * Instantiates a new bi gram utility.
	 *
	 * @param dataDelim - The delimiter using which data is to be tokenized
	 * @param endnodeStr - The String which is used to buffer / pad the single token elements to generate a combination.
	 */
	public BiGramUtility(String dataDelim, String endnodeStr)
	{
		super();
		this.dataDelim = dataDelim;
		this.endnodeStr = endnodeStr;
	}

	/**
	 * Generates bi grams.
	 *
	 * @author Kartik Iyer
	 * @param termString the term string
	 * @return the list
	 */
	public List<String> generateBiGram(String termString)
	{
		if (termString == null || termString.trim().equals(""))
			return null;

		String[] tokens = termString.split(dataDelim);
		List<String> termNodes = new ArrayList<String>(1);
		String linkTerm = null;

		for (int j = 0; j < tokens.length; j++)
		{
			String token = tokens[j];

			if (linkTerm == null)
			{
				linkTerm = token;
				continue;
			}
			if (linkTerm.equals(token))
				continue;

			String biGramTerm = linkTerm + dataDelim + token;
			linkTerm = token;
			termNodes.add(biGramTerm);
		}

		if (termNodes.isEmpty() && linkTerm != null)
			termNodes.add(linkTerm + dataDelim + endnodeStr);

		return termNodes;
	}
}

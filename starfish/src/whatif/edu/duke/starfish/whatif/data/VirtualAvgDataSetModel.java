package edu.duke.starfish.whatif.data;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * This data set model expects the input specifications as input from an XML
 * file. It also averages out the data across the reducers.
 * 
 * @author hero
 */
public class VirtualAvgDataSetModel extends DataSetModel {

	private List<MapInputSpecs> inputSpecs;

	/**
	 * Constructor
	 * 
	 * @param inputSpecs
	 *            the input specifications
	 */
	public VirtualAvgDataSetModel(List<MapInputSpecs> inputSpecs) {
		this.inputSpecs = inputSpecs;
	}

	/**
	 * @see edu.duke.starfish.whatif.data.DataSetModel#generateMapInputSpecs(Configuration)
	 */
	@Override
	public List<MapInputSpecs> generateMapInputSpecs(Configuration conf) {
		return inputSpecs;
	}
}

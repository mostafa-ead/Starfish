package edu.duke.starfish.whatif.data;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * A dataset model that always returns a fixed list of map input specifications
 * 
 * @author hero
 */
public class FixedInputSpecsDataSetModel extends DataSetModel {

	List<MapInputSpecs> specs;

	/**
	 * Constructor
	 * 
	 * @param specs
	 *            the list of map input specifications
	 */
	public FixedInputSpecsDataSetModel(List<MapInputSpecs> specs) {
		this.specs = specs;
	}

	/**
	 * @see edu.duke.starfish.whatif.data.DataSetModel#generateMapInputSpecs(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public List<MapInputSpecs> generateMapInputSpecs(Configuration conf) {
		return specs;
	}

}

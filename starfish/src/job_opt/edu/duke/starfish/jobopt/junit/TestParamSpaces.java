package edu.duke.starfish.jobopt.junit;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import edu.duke.starfish.jobopt.params.BooleanParamDescriptor;
import edu.duke.starfish.jobopt.params.DoubleParamDescriptor;
import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.params.IntegerParamDescriptor;
import edu.duke.starfish.jobopt.params.ParamTaskEffect;
import edu.duke.starfish.jobopt.space.MultiJobParamSpacePoint;
import edu.duke.starfish.jobopt.space.MultiJobParameterSpace;
import edu.duke.starfish.jobopt.space.ParameterSpace;

/**
 * Test the parameter space classes
 * 
 * @author hero
 * 
 */
public class TestParamSpaces extends TestCase {

	@Test
	public void testMultiJobParamSpace1() {

		// 2 discrete spaces
		ParameterSpace space1 = new ParameterSpace();
		space1.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMPRESS_OUT, ParamTaskEffect.EFFECT_MAP));

		ParameterSpace space2 = new ParameterSpace();
		space2.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMBINE, ParamTaskEffect.EFFECT_BOTH));
		space2.addParameterDescriptor(new IntegerParamDescriptor(
				HadoopParameter.SORT_FACTOR, ParamTaskEffect.EFFECT_MAP, 2, 5));

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);
		multiSpace.addParamSpace(2, space2);

		assertEquals(3, multiSpace.getNumDimensions());
		assertEquals(16, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 16);
		assertEquals(16, points.size());
		assertEquals(
				"ParameterSpacePoint [values={mapred.output.compress=false}]",
				points.get(0).getJobSpacePoint(1).toString());
		assertEquals(
				"ParameterSpacePoint [values={io.sort.factor=2, starfish.use.combiner=false}]",
				points.get(0).getJobSpacePoint(2).toString());
	}

	@Test
	public void testMultiJobParamSpace2() {

		// 1 discrete space and 1 continuous space
		ParameterSpace space1 = new ParameterSpace();
		space1.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMPRESS_OUT, ParamTaskEffect.EFFECT_MAP));

		ParameterSpace space2 = new ParameterSpace();
		space2.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMBINE, ParamTaskEffect.EFFECT_BOTH));
		space2.addParameterDescriptor(new DoubleParamDescriptor(
				HadoopParameter.RED_IN_BUFF_PERC,
				ParamTaskEffect.EFFECT_REDUCE, 0, 1));

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);
		multiSpace.addParamSpace(2, space2);

		assertEquals(3, multiSpace.getNumDimensions());
		assertEquals(Integer.MAX_VALUE, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 16);

		assertEquals(64, points.size());
		assertEquals(
				"ParameterSpacePoint [values={mapred.output.compress=false}]",
				points.get(0).getJobSpacePoint(1).toString());
		assertEquals(
				"ParameterSpacePoint [values={mapred.job.reduce.input.buffer.percent=0.0, starfish.use.combiner=false}]",
				points.get(0).getJobSpacePoint(2).toString());
	}

	@Test
	public void testMultiJobParamSpace3() {

		// 1 empty space and 1 discrete space
		ParameterSpace space1 = new ParameterSpace();

		ParameterSpace space2 = new ParameterSpace();
		space2.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMBINE, ParamTaskEffect.EFFECT_BOTH));
		space2.addParameterDescriptor(new IntegerParamDescriptor(
				HadoopParameter.SORT_FACTOR, ParamTaskEffect.EFFECT_MAP, 2, 5));

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);
		multiSpace.addParamSpace(2, space2);

		assertEquals(2, multiSpace.getNumDimensions());
		assertEquals(8, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 8);

		assertEquals(8, points.size());
		assertEquals("ParameterSpacePoint [values={}]", points.get(0)
				.getJobSpacePoint(1).toString());
		assertEquals(
				"ParameterSpacePoint [values={io.sort.factor=2, starfish.use.combiner=false}]",
				points.get(0).getJobSpacePoint(2).toString());
	}

	@Test
	public void testMultiJobParamSpace4() {

		// 1 discrete space and 1 empty space
		ParameterSpace space1 = new ParameterSpace();
		space1.addParameterDescriptor(new BooleanParamDescriptor(
				HadoopParameter.COMBINE, ParamTaskEffect.EFFECT_BOTH));
		space1.addParameterDescriptor(new IntegerParamDescriptor(
				HadoopParameter.SORT_FACTOR, ParamTaskEffect.EFFECT_MAP, 2, 5));

		ParameterSpace space2 = new ParameterSpace();

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);
		multiSpace.addParamSpace(2, space2);

		assertEquals(2, multiSpace.getNumDimensions());
		assertEquals(8, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 8);

		assertEquals(8, points.size());
		assertEquals(
				"ParameterSpacePoint [values={io.sort.factor=2, starfish.use.combiner=false}]",
				points.get(0).getJobSpacePoint(1).toString());
		assertEquals("ParameterSpacePoint [values={}]", points.get(0)
				.getJobSpacePoint(2).toString());
	}

	@Test
	public void testMultiJobParamSpace5() {

		// 2 empty spaces
		ParameterSpace space1 = new ParameterSpace();
		ParameterSpace space2 = new ParameterSpace();

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);
		multiSpace.addParamSpace(2, space2);

		assertEquals(0, multiSpace.getNumDimensions());
		assertEquals(1, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 2);

		assertEquals(1, points.size());
		assertEquals("ParameterSpacePoint [values={}]", points.get(0)
				.getJobSpacePoint(1).toString());
		assertEquals("ParameterSpacePoint [values={}]", points.get(0)
				.getJobSpacePoint(2).toString());
	}

	@Test
	public void testMultiJobParamSpace6() {

		// 1 empty space
		ParameterSpace space1 = new ParameterSpace();

		MultiJobParameterSpace multiSpace = new MultiJobParameterSpace();
		multiSpace.addParamSpace(1, space1);

		assertEquals(0, multiSpace.getNumDimensions());
		assertEquals(1, multiSpace.getNumUniqueSpacePoints());

		List<MultiJobParamSpacePoint> points = multiSpace.getSpacePointGrid(
				false, 2);

		assertEquals(1, points.size());
		assertEquals("ParameterSpacePoint [values={}]", points.get(0)
				.getJobSpacePoint(1).toString());
	}

}

package edu.duke.starfish.jobopt.params;

/**
 * Describes what kind of tasks a particular parameter can effect.
 * 
 * @author hero
 */
public enum ParamTaskEffect {

	EFFECT_NONE, // Effects neither map nor reduce tasks (may effect job)
	EFFECT_MAP, // Effects only map tasks
	EFFECT_REDUCE, // Effects only reduce tasks
	EFFECT_BOTH;// Effects both map and reduce tasks
}

package edu.duke.starfish.visualizer.view;

import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.shape.Rectangle;
import javafx.scene.shape.Line;
import javafx.scene.text.Text;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.scene.paint.LinearGradient;
import javafx.scene.paint.Stop;
import javafx.scene.control.ScrollBar;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;
import javafx.util.Math;

import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.*;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;

import edu.duke.starfish.visualizer.model.timeline.*;
import edu.duke.starfish.visualizer.view.custom.BorderGroup;
import edu.duke.starfish.visualizer.view.custom.ToolTip;
import edu.duke.starfish.visualizer.view.Constants.*;


/**
 * Represents a tick mark and label on the x-axis
 */
class XTickMark extends CustomNode {
    var index : Integer;
    var buffer : Integer;
    
    public override function create(): Node {
		return Group {
			content : [
			// The tick mark
			Line {
				startX : bind X_ORIGIN + buffer + index * ((X_END-X_ORIGIN) / maxXAxisMins)
				startY : bind Y_ORIGIN + buffer
				endX : bind X_ORIGIN + buffer + index * ((X_END-X_ORIGIN) / maxXAxisMins)
				endY : bind Y_ORIGIN + 8
				strokeWidth : 3
			}
			// The label
			Text {
				x : bind X_ORIGIN + index * ((X_END-X_ORIGIN) / maxXAxisMins) - 2
				y : bind Y_ORIGIN + 24
				font : COMMON_FONT_12
				content : (index as Integer).toString();
			}
		]};
    }
}

/**
 * Represents a tick mark and label on the y-axis
 */
class YTickMark extends CustomNode {
    var index : Integer;
    var host : String;
    var pixelsPerHost : Number;
        
    public override function create(): Node {
		return Group {
			content : [
			// The tick mark
			Line {
				startX : bind X_ORIGIN - 8
				startY : bind Y_ORIGIN - (pixelsPerHost * (1+index))
				endX : bind X_ORIGIN - 4
				endY : bind Y_ORIGIN - (pixelsPerHost * (1+index))
				translateY: bind (scroll.max-scroll.value) * pixelsPerHost;
				strokeWidth : 3
				visible : bind ((scroll.max-scroll.value)*pixelsPerHost + Y_ORIGIN-(pixelsPerHost*(1+index))) >= Y_END 
								and ((scroll.max-scroll.value)*pixelsPerHost + Y_ORIGIN-(pixelsPerHost*(1+index))) < Y_ORIGIN
			}
			// A horizontal dotted line
			Line {
				startX : bind X_ORIGIN
				startY : bind Y_ORIGIN - (pixelsPerHost * (1+index))
				endX : bind X_END + 2
				endY : bind Y_ORIGIN - (pixelsPerHost * (1+index))
				translateY: bind (scroll.max-scroll.value)*pixelsPerHost;
				strokeWidth : 1
				strokeDashArray : [1.0 .. 4.0]
				visible : bind ((scroll.max-scroll.value)*pixelsPerHost + Y_ORIGIN-(pixelsPerHost*(1+index))) >= Y_END
								and ((scroll.max-scroll.value)*pixelsPerHost + Y_ORIGIN-(pixelsPerHost*(1+index))) < Y_ORIGIN
			}
			// The label
			Text {
				x : bind X_ORIGIN - 100
				y : bind Y_ORIGIN - (pixelsPerHost * (index + 0.65))
				translateY: bind (scroll.max-scroll.value) * pixelsPerHost;
				font : COMMON_FONT_12
				content : host as String
				wrappingWidth : 90
				visible : bind ((scroll.max-scroll.value)*pixelsPerHost + Y_ORIGIN - (pixelsPerHost * (index + 0.65))) > Y_END
							   and ((scroll.max-scroll.value)*pixelsPerHost + (Y_ORIGIN - (pixelsPerHost * (index + 0.65)) + 20) < Y_ORIGIN)
			}
		]};
    }
}

/**
 * Represents a tick mark and label on the y-axis
 */
class Task extends CustomNode {
    var hostIndex : Integer;
    var slotIndex : Integer;
    var pixelsPerHost : Number;
    var pixelsPerSlot : Number;
    var attempt : MRTaskAttemptInfo;
        
    public override function create(): Node {
		var taskId = attempt.getTruncatedTaskId();
		var toolTipContent = getToolTipContent();
		
		var toolTipWidth = getToolTipWidth(); // Temp until we migrate to JavaFX 1.3
		var toolTipHeight = getToolTipHeight(); // Temp until we migrate to JavaFX 1.3
		
		var group = Group { content : [] };
		    
		// The rectangle
		insert Rectangle {
			x: bind getXCoordinate(X_ORIGIN, X_END);
			y: bind getYCoordinate(Y_ORIGIN);
			width: bind getRectangleWidth(X_ORIGIN, X_END);
			height: bind pixelsPerSlot - 4
			fill: createGradient(getTaskColor())
			stroke: Color.BLACK
			arcWidth: 10  
			arcHeight: 10
			opacity: 0.9
			
			translateY: bind (scroll.max-scroll.value) * pixelsPerHost;				
			visible : bind ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN)) >= Y_END 
							and ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN) + pixelsPerSlot - 4) < Y_ORIGIN

			onMouseMoved: function( e: MouseEvent ): Void {
			    // Show the toolTip
			    toolTip.show(toolTipContent, e.x, e.y, 0, (scroll.max-scroll.value) * pixelsPerHost, toolTipWidth, toolTipHeight);
			}
			
			onMouseExited : function(e : MouseEvent) {
			    // Hid the toolTip
			    toolTip.hide();
			}
			
		} into group.content;
		
		if (attempt instanceof MRReduceAttemptInfo) {
		    var redAttempt : MRReduceAttemptInfo = attempt as MRReduceAttemptInfo;
		    
			// A vertical dotted line denoting the shuffle end time
			insert Line {
				startX : bind getXCoordinate(X_ORIGIN, X_END) + 
							convertTimeDiffToPixels(X_ORIGIN, X_END, redAttempt.getStartTime().getTime(), redAttempt.getShuffleEndTime().getTime())
				startY : bind getYCoordinate(Y_ORIGIN) + 1;
				endX : bind getXCoordinate(X_ORIGIN, X_END) + 
							convertTimeDiffToPixels(X_ORIGIN, X_END, redAttempt.getStartTime().getTime(), redAttempt.getShuffleEndTime().getTime())
				endY : bind getYCoordinate(Y_ORIGIN) + pixelsPerSlot - 5
				fill : Color.PURPLE
				strokeWidth : 2
				strokeDashArray : [4, 6]
				opacity : 0.7
				
				translateY: bind (scroll.max-scroll.value) * pixelsPerHost;				
				visible : bind ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN)) >= Y_END
							and ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN) + pixelsPerSlot - 4) < Y_ORIGIN
			} into group.content;
		}
		
		// The attempt id
		insert Text {
			x: bind getXCoordinate(X_ORIGIN, X_END) + (getRectangleWidth(X_ORIGIN, X_END) / 2) - taskId.length() * 3.5;
			y: bind getYCoordinate(Y_ORIGIN) + (pixelsPerSlot - 4) * 0.65
			font : COMMON_FONT_12
			content : taskId;
			fill : Color.WHITE
			
			translateY: bind (scroll.max-scroll.value) * pixelsPerHost;				
			visible : bind taskId.length() * 7 + 20 < getRectangleWidth(X_ORIGIN, X_END) 
						and ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN)) >= Y_END 
						and ((scroll.max-scroll.value)*pixelsPerHost + getYCoordinate(Y_ORIGIN) + pixelsPerSlot - 4) < Y_ORIGIN
		} into group.content;
		
		return group;
    }

	// Convert the time difference to number of pixels
    function convertTimeDiffToPixels(X_ORIGIN: Number, X_END: Number, startTime: Long, endTime: Long): Number {
	    return ((X_END-X_ORIGIN) / maxXAxisMins) * (endTime - startTime) / 60000;
    }

	// Get the width of the task rectangle
	function getRectangleWidth(X_ORIGIN: Number, X_END: Number): Number {
	    return ((X_END-X_ORIGIN) / maxXAxisMins) * attempt.getDuration() / 60000 - 1
	}

    // Get the x-coordinate of the rectangle
	function getXCoordinate(X_ORIGIN: Number, X_END: Number): Number {
	    return X_ORIGIN + convertTimeDiffToPixels(X_ORIGIN, X_END, jobStartTime, attempt.getStartTime().getTime()) + 2
	}
    
    // Get the y-coordinate of the rectangle
    function getYCoordinate(Y_ORIGIN: Number): Number {
        return Y_ORIGIN - (hostIndex * pixelsPerHost + (1+slotIndex) * pixelsPerSlot) + 2;
    }
    
	// get the color for a task
	function getTaskColor() : Color {
	    if (attempt.getStatus() == MRExecutionStatus.FAILED or attempt.getStatus() == MRExecutionStatus.KILLED) {
	        // Failed task
	    	return Color.RED;  
		} else if (attempt instanceof MRMapAttemptInfo) then {
		    // Map task
			return Color.BLUE; 
	    } else if (attempt instanceof MRReduceAttemptInfo) {
	        // Reduce task
			return Color.PURPLE;
		} else if(attempt instanceof MRSetupAttemptInfo) then {
		    // Setup task
			return Color.MAGENTA; 
		} else if (attempt instanceof MRCleanupAttemptInfo) then {
		    // Cleanup task
			return Color.ORANGE;
		} else {
		    return Color.BLACK;
		}
	}
	
    // Create a light to dark gradient
    function createGradient(baseColor: Color) : LinearGradient { 
		return LinearGradient {
			stops: [
				Stop {
					offset: 0.0,
					color: baseColor.ofTheWay(Color.WHITE, 0.35) as Color
				},
				Stop {
					offset: 1.0,
					color: baseColor.ofTheWay(Color.BLACK, 0.35) as Color
				}
			]
		}
    }
	
	// Create the toolTip content
	function getToolTipContent(): String {
	    var toolTipContent : String = "";
	    
		if (attempt instanceof MRMapAttemptInfo) then {
		    toolTipContent = "Map Attempt:  {attempt.getTruncatedTaskId()}";
			toolTipContent = "{toolTipContent}\nDuration:  {ProfileUtils.getFormattedDuration(attempt.getDuration())}";
			toolTipContent = "{toolTipContent}\nLocality:  {(attempt as MRMapAttemptInfo).getDataLocality().getDescription()}";
			
	    } else if (attempt instanceof MRReduceAttemptInfo) {
	        toolTipContent = "Reduce Attempt:  {attempt.getTruncatedTaskId()}";
	        toolTipContent = "{toolTipContent}\nShuffle Duration:  {ProfileUtils.getFormattedDuration((attempt as MRReduceAttemptInfo).getShuffleDuration())}";
	        toolTipContent = "{toolTipContent}\nTotal Duration:  {ProfileUtils.getFormattedDuration(attempt.getDuration())}";
	        
		} else if(attempt instanceof MRSetupAttemptInfo) then {
		    toolTipContent = "Setup Attempt:  {attempt.getTruncatedTaskId()}";
		    toolTipContent = "{toolTipContent}\nDuration:  {ProfileUtils.getFormattedDuration(attempt.getDuration())}";
		     
		} else if (attempt instanceof MRCleanupAttemptInfo) then {
		    toolTipContent = "Cleanup Attempt:  {attempt.getTruncatedTaskId()}";
		    toolTipContent = "{toolTipContent}\nDuration:  {ProfileUtils.getFormattedDuration(attempt.getDuration())}";
		    
		}
	    
	    return toolTipContent;
	}
	
	// TEMP until we migrate to JavaFX 1.3
	function getToolTipWidth(): Number {
	    
		if (attempt instanceof MRMapAttemptInfo) then {
			return 180;
	    } else if (attempt instanceof MRReduceAttemptInfo) {
	        return 210;
		} else {
		    return 180;
		}
	}

	// TEMP until we migrate to JavaFX 1.3
	function getToolTipHeight(): Number {
	    
		if (attempt instanceof MRMapAttemptInfo) then {
			return 42;
	    } else if (attempt instanceof MRReduceAttemptInfo) {
	        return 42;
		} else {
		    return 28;
		}
	}
}
/*
 *
 * @author Hero (hero@cs.duke.edu)
 * 
 * TimelineView.fx
 *
 * Presents the timeline execution of the tasks
 *
 */
public class TimelineView extends AppView {
    
    /**
     * CONSTANTS
     **/
    var X_BOX = 55;
    var Y_BOX = 50;
    var BOX_HEIGHT = 85;
    
	var X_ORIGIN : Number = 120;
    var Y_ORIGIN : Number = bind height - 100;
    var X_END : Number = bind width - 100;
    var Y_END : Number = 145;

	/**
	 * Job related variables
	 **/
	var model : TimelineModel;
	var numHosts : Number;
	var numSlots : Integer;
	var jobStartTime : Long;
	var jobEndTime : Long;
	var maxXAxisMins : Number;
	
	/**
	 * Main Nodes
	 **/
	var errorText: Text;
	
	var summaryBox : Group;
	
	var xAxes : Group;
	var yAxes : Group;
	var timeline : Group;
	var scroll : ScrollBar;
	
	var toolTip: ToolTip;
	
	// A simple rectangle to capture the mouse wheel movement over the graph
	var scrollablePane = Rectangle {
	    x: bind X_ORIGIN - 110,
		y: bind Y_END,
	    width: bind X_END - X_ORIGIN + 120,
		height: bind Y_ORIGIN - Y_END + 2,
	    fill: Color.WHITE
	    
		onMouseWheelMoved: function(event) {
			scroll.value += event.wheelRotation
		}
	}
	
	
	/***********************************************************************
	 * HELPER METHODS
	 * ********************************************************************/
	
	function getTimeDiffInPixels(endTime : Long, startTime : Long) : Number {
		return ((X_END-X_ORIGIN)*(endTime-startTime)) / maxXAxisMins;
	}

	/***********************************************************************
	 * DRAWING METHODS
	 * ********************************************************************/
	
	function createSummaryBox(): Void {
	    
	    summaryBox = Group {
	        content : [
	        	BorderGroup {
	        	    groupX : bind X_BOX
	        	    groupY : bind Y_BOX
	        	    groupWidth : bind width - (2 * X_BOX)
	        	    groupHeight : bind BOX_HEIGHT
	        	    groupTitle : "Job Summary"
	        	    textWidth : 106.7
	        	}
	        	 Group {
	        	     content: [
	        	     	// VBox with row headers
		        	    VBox {
			        	    layoutX: X_BOX + 15
	        	     		layoutY: Y_BOX + 15
		        	        spacing: 5
		        	        content: [
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Job Total Duration: "
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Average Map Duration: "
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Average Reduce Duration: "
			        	         }
		        	         ]
		        	    }
		        	     // VBox with durations
		        	     VBox {
			        	    layoutX: X_BOX + 200
	        	     		layoutY: Y_BOX + 16
		        	        spacing: 7
		        	        content: [
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "{ProfileUtils.getFormattedDuration(
					        	     				job.getDuration())}"
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "{ProfileUtils.getFormattedDuration(
					        	     				ProfileUtils.calculateDurationAverage(
					        	     					job.getMapTasks()))}"
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "{ProfileUtils.getFormattedDuration(
					        	     				ProfileUtils.calculateDurationAverage(
					        	     					job.getReduceTasks()))}"
			        	         }
		        	         ]
		        	     }
	        	     	// VBox with second row headers
		        	    VBox {
			        	    layoutX: X_BOX + 330
	        	     		layoutY: Y_BOX + 15
		        	        spacing: 5
		        	        content: [
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Job"
					        	     fill: Color.WHITE
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Number of Map Tasks: "
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Number of Reduce Tasks: "
			        	         }
		        	         ]
		        	     }
		        	     // VBox with number of tasks
		        	     VBox {
			        	    layoutX: X_BOX + 500
	        	     		layoutY: Y_BOX + 16
		        	        spacing: 7
		        	        content: [
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "Job"
					        	     fill: Color.WHITE
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "{job.getMapTasks().size()}"
			        	         }
			        	         Text {
					        	     font: COMMON_FONT_13
					        	     content: "{job.getReduceTasks().size()}"
			        	         }
		        	         ]
		        	     }
	        	     ]
	        	 }
	        ]
	    }
	}
	
	/**
	 * Create the scroll bar
	 **/
	function createScrollBar(): Void {
	   scroll = ScrollBar {
	    		translateX : bind X_END + 4
	    		translateY : bind Y_END - 4
	    		height : bind Y_ORIGIN - Y_END + 8
	    
	    		blockIncrement : 1
	    		unitIncrement : 1
	    		clickToPosition : false
	    		min : 0
	    		max : bind numHosts - (Y_ORIGIN-Y_END) / Math.max(25 * numSlots, (Y_ORIGIN-Y_END) / numHosts)
	    		value : numHosts - (Y_ORIGIN-Y_END) / Math.max(25 * numSlots, (Y_ORIGIN-Y_END) / numHosts)
	    		vertical : true
	    		blocksMouse : false
	    	}
	}


	/**
	 * create x axes and labels
	 */
	function createXAxes() : Void {
	    
	    // Calculate the ideal number of tick marks
	    var totalNumMins : Double = (jobEndTime - jobStartTime) / 60000.0;
	    var suggestedNumTicks : Double = Math.round((X_END-X_ORIGIN) / 50);
	    
	    var minUnit : Integer = 1;
	    var minDiff : Double = Double.MAX_VALUE;
	    for (unit in [1,2,5,10,20,50,100]) {
	        if (Math.abs(totalNumMins / suggestedNumTicks - unit) < minDiff) {
	            minUnit = unit;
	            minDiff = Math.abs(totalNumMins / suggestedNumTicks - unit);
	        }
	    }

		var numTicks = Math.ceil(totalNumMins / minUnit);
		maxXAxisMins = numTicks * minUnit;

		// Create the x-axis
		var xTicksArray = [0..(numTicks as Integer)];
		var buffer = 2;
		
		xAxes = Group {
			content : [
				// x axes
				Line {
					startX : bind X_ORIGIN - buffer
					startY : bind Y_ORIGIN + buffer
					endX : bind X_END + buffer 
					endY : bind Y_ORIGIN + buffer
					strokeWidth : 4
				}
				// x axes title: under the x line
				Text {
					x : bind (X_END - X_ORIGIN) * 0.6
					y : bind Y_ORIGIN + 50
					font : COMMON_FONT_14
					content : "Time (minutes)"
				}
				// use a tiny segment to identify x scale
				for(tick in xTicksArray) {
					XTickMark {
					    index : tick * minUnit
					    buffer : buffer
					}
				}
			]
		};
	}
	
	/**
	 * create y axes and labels
	 */
	function createYAxes() : Void {

		yAxes = Group {
			content : [
				// The Y axis
				Line {
					startX : bind X_ORIGIN - 2
					startY : bind Y_ORIGIN - 2
					endX : bind X_ORIGIN - 2
					endY : bind Y_END + 1
					strokeWidth : 4
				}
				// show the Y axis tick marks and labels
				for(host in model.getTimelineTaskTrackers()) {
					YTickMark {
					    index : indexof host
					    host : host.getHostName() as String
					    pixelsPerHost : bind Math.max(25 * numSlots, (Y_ORIGIN-Y_END) / numHosts)
					}
				}
			]
		};
	}
	
	/**
	 * Creates all the task rectangles in the timeline plot
	 */
	function createTimelinePlot() : Void {
	    
		timeline = Group { content : [] };
		var hostId : Integer;
		var slotId : Integer;
		
		// Iterate over all attempts in all slots in all trackers
		for (tracker in model.getTimelineTaskTrackers()) {
		    hostId = tracker.getHostId();
		    
		    for (slot in tracker.getAllSlots()) {
		        slotId = slot.getSlotId();
		        
		        for (attempt in slot.getAttempts()) {
		            // Create a Task rectangle for each attempt
		            insert Task {
					    hostIndex : hostId
					    slotIndex : slotId
					    pixelsPerHost : bind Math.max(25 * numSlots, (Y_ORIGIN-Y_END) / numHosts)
					    pixelsPerSlot : bind Math.max(25 * numSlots, (Y_ORIGIN-Y_END) / numHosts) / numSlots
					    attempt : attempt
					}
					into timeline.content;
		        }
		    }
		}
		
		// Create the toolTip
		toolTip = ToolTip { };
		
		insert toolTip into timeline.content;
	}
	
	/***********************************************************************
	 * MAIN METHODS
	 * ********************************************************************/

    /**
	 * create View, the general work includes collecting data, init the hosts, render the layout 
	 */
	override function createView(): Void {
	    
	    if (not manager.loadTaskDetailsForMRJob(job)) {
	        errorText = Text {
				content : "Unable to load the job information. Please check the logs for more details!"
				x : 40
				y : 75
				wrappingWidth: bind width - 40
				font : COMMON_FONT_16
	        }
	        return;
	    }
	    
	    // Set the job data
	    model = new TimelineModel(job);
	    numHosts = model.getNumberTaskTrackers();
	    numSlots = model.getNumberTaskSlotsPerTaskTracker();
	    
		jobStartTime = job.getStartTime().getTime();
		jobEndTime = job.getEndTime().getTime();
		
		// Create the view
	    createSummaryBox();
	    createScrollBar();
	    createXAxes();
	    createYAxes();
	    createTimelinePlot()
	}
	
	/*
	 * Construct function 
	 */
    override function create():Node {
        Group {
            content: bind [
            	summaryBox,
            	scrollablePane,
	 			xAxes,
	 			yAxes,
	            scroll,
	            timeline,
	            errorText
            ]
       };        
    }
}

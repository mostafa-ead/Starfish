package edu.duke.starfish.visualizer.view;

import javafx.stage.Stage;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.transform.Rotate;
import javafx.scene.paint.Color;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.part.CategoryAxis;
import javafx.scene.chart.part.NumberAxis;
import javafx.scene.layout.VBox;
import javafx.scene.Cursor;
import javafx.scene.layout.LayoutInfo;
import javafx.scene.shape.Line;
import javafx.scene.text.FontWeight;
import javafx.scene.input.MouseEvent;
import javafx.util.Math;

import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.visualizer.model.Histogram;
import edu.duke.starfish.visualizer.view.custom.BorderGroup;
import edu.duke.starfish.visualizer.view.custom.ToolTip;
import edu.duke.starfish.visualizer.view.Constants.*;


/**
 * @author Fei Dong (dongfei@cs.duke.edu), Herodotos Herodotou (hero@cs.duke.edu)
 * 
 * DataSkewView.fx
 * 
 * viewing aggregated task input/output during map/reduce phases
 */
 
/**
 * A custom histogram chart
 */
public class HistogramChart extends CustomNode {
    
    // Layout data
    public var x: Number;
    public var y: Number;
    public var width: Number;
    public var height: Number;
    public var title: String;
    
    // Y-axis data
    public var labelY: String;
    public var frequencies: Integer[];
        
    // X-axis data
    public var minValue: Number;
    public var maxValue: Number;
    public var valueUnit: Number;
    public var color: Color;
    
 	// Create the histogram
    override function create():Node {
        
        // Create the series
	    var series = BarChart.Series {};
        for (freq in frequencies) {
            insert BarChart.Data {
                value: freq
                fill: color
            } into series.data;
        }
        
        // Create fantom categories to display the bars correctly
	    var categories: String[];
	    for (index in [1..sizeof frequencies]) {
			insert index.toString() into categories; 
	    }
	    
	    // Find max frequency and unit
	    var maxFreq: Integer = 0;
        for (freq in frequencies) {
            if (freq > maxFreq)
            	maxFreq = freq;
        }
        maxFreq = maxFreq + 2;
        var freqUnit = Math.max(1, maxFreq / 10);
        
	    // Create the chart
	    var chart = BarChart {
	        title: title
	        titleFont: Font.font(Font.DEFAULT.family, FontWeight.BOLD, 18)
	        titleGap: 0

	        width: bind width
	        height: bind height
	        horizontalGridLineVisible: true
	        legendVisible: false
	        barGap : 0
	        categoryGap : 0

			data: bind series
	        	        
	        categoryAxis: CategoryAxis {
	            categories: categories
	            tickLabelsVisible: false
	        }
	        
	        valueAxis: NumberAxis {
	            label: labelY
	            lowerBound: 0
	            upperBound: maxFreq
	            tickUnit: freqUnit
	            labelFont: COMMON_FONT_13
	            tickLabelFont: COMMON_FONT_11
	            minorTickVisible: false
	            formatTickLabel: function(val) {(Math.round(val) as Integer).toString()}
	        }
	        
	    };
	    
	 	var xAxis = CustomNumberAxis {     
				axisX: bind chart.categoryAxis.layoutX + 15;
				axisY: bind chart.categoryAxis.layoutY + 31;
				axisWidth: bind chart.categoryAxis.width - 10;
				axisLabel: "Data Size (MB)";
				 
				minValue: minValue;
				maxValue: maxValue;
				unit: valueUnit;
	 	}
        
        Group {
            layoutX: bind x
            layoutY: bind y
            content: [chart, xAxis]
        }
    }
    
}

/**
 * A custom number horizontal axis for the histogram
 */
public class CustomNumberAxis extends CustomNode {
    
    public var axisX: Number;
    public var axisY: Number;
    public var axisWidth: Number;
    public var axisLabel: String;
    
    public var minValue: Number;
    public var maxValue: Number;
    public var unit: Number;
    
    // Create the axis
    override function create():Node {
        
	    var valueRange: Number = maxValue - minValue;
	    var numTicks: Integer = Math.ceil(valueRange / unit) as Integer;
	    var scale: Number = bind axisWidth / valueRange;

	    if (numTicks * unit == valueRange) {
	   		numTicks = numTicks + 1;
	   	}
	   	
		return Group {		    
			content : [
				// x axes label: under the x line
				Text {
					x : bind axisX + axisWidth * 0.4
					y : bind axisY + 40
					font : COMMON_FONT_12
					content : axisLabel
				}
				// use a tiny segment to identify x scale
				for(tick in [0..(numTicks-1)]) {
					// The tick mark
					Line {
						startX : bind axisX + tick * unit * scale
						startY : bind axisY
						endX : bind axisX + tick * unit * scale
						endY : bind axisY + 9
						strokeWidth : 1
						fill : Color.LIGHTGREY
						opacity : 0.5
					}
				}
				for(tick in [0..(numTicks-1)]) {
					// The tick label
					var label = (Math.round(minValue + tick * unit) as Integer).toString();
					Label {
						layoutX : bind axisX + tick * unit * scale - label.length() * 3
						layoutY : bind axisY + 11
						font : COMMON_FONT_11
						opacity : 0.8
						text : label;
					}
				}
			]
		};

    }
}


/**
 * The main body 
 */
public class DataSkewView extends AppView {
    
    ///////////////////////
    // Model's variables //
    ///////////////////////
    def titles = ["Map Input Data Histogram", 
    			  "Map Output Data Histogram", 
    			  "Reduce Input Data Histogram", 
    			  "Reduce Output Data Histogram"];
    var numBuckets = 10;
	
	
    //////////////////////
    // View's variables //
    //////////////////////
    var errorText: Text;
    var barChart: HistogramChart;
    var buttons: VBox;
    var toolTip: ToolTip;
    var borderInfo: BorderGroup;
    var infoText: Text;

	// Layout info
	var buttonsX = 20;
	var buttonsY = 100;
	var buttonWidth = 133;
	
	var chartX = buttonWidth + 30;
	var chartY = bind Math.max(30, Math.min(40, height / 20)) + 10;
	var chartWidth = bind width - chartX - 20;
	var chartHeight = bind height - chartY - 100;

    ////////////////////////////
    // object functions       //
    ////////////////////////////
        
    /**
     * Create the bar chart
     * 
     * type = 0: map input
     * type = 1: map output
     * type = 2: reduce input
     * type = 3: reduce output
     */
    function createHistogramChart(type: Integer): Void {

		// Gather the data
	    var data: Double[];
	    var zero: Long = 0;
	    var label: String;
	    var color: Color;
	    var numTasks: Integer = 0;
	    var totalSize: Number = 0;
        
		// handle map data
        if (type == 0 or type == 1) {
            label = "Number of map tasks";
            color = Color.BLUE;
            
            for (mrMap in job.getMapAttempts(MRExecutionStatus.SUCCESS)) {
                var profile = mrMap.getProfile();
    			var bytes: Number;
    			if (type == 0) {
	   				bytes = profile.getCounter(MRCounter.MAP_INPUT_BYTES, zero);
    			} else {
    			    bytes = profile.getCounter(MRCounter.MAP_OUTPUT_BYTES, zero);
    			}
    			insert bytes / 1048576 into data;
    			
    			++numTasks;
    			totalSize += bytes;
            }
        }

        // handle reduce data
        if (type == 2 or type == 3) {
            label = "Number of reduce tasks";
            color = Color.PURPLE;
            
            for (mrReduce in job.getReduceAttempts(MRExecutionStatus.SUCCESS)) {
                var profile = mrReduce.getProfile();
				var bytes: Number;   		 			
    			if (type == 2) {
	   				bytes = profile.getCounter(MRCounter.REDUCE_INPUT_BYTES, zero);
    			} else {
    			    bytes = profile.getCounter(MRCounter.REDUCE_OUTPUT_BYTES, zero);
    			}
    			insert bytes / 1048576 into data;
    			
    			++numTasks;
    			totalSize += bytes;
            }
        }

		// Create the histogram data
	    var histogram = new Histogram(data, numBuckets);
	    
		// Create the histogram chart
	  	barChart = HistogramChart {
		    // Layout data
		    x: bind chartX;
		    y: bind chartY;
		    width: bind chartWidth;
		    height: bind chartHeight;
		    title: titles[type];
		    
		    // Y-axis data
		    labelY: label;
		    frequencies: histogram.getFrequencies();
		        
		    // X-axis data
		    minValue: histogram.getMinValue();
		    maxValue: histogram.getMaxValue();
		    valueUnit: histogram.getUnit();
		    color: color;
	  	};
	  	
	  	// Create the info text
	  	var info: String;
	  	if (type == 0) {
	  	    info = 	"Map tasks: \n    {numTasks}\n \n"
	  	    		"Total input size: \n    {ProfileUtils.getFormattedSize(totalSize)}";
	  	}
	  	else if (type == 1) {
	  	    info = 	"Map tasks: \n    {numTasks}\n \n"
	  	    		"Total output size: \n    {ProfileUtils.getFormattedSize(totalSize)}";
	  	}
	  	else if (type == 2) {
	  	    info = 	"Reduce tasks: \n    {numTasks}\n \n"
	  	    		"Total input size: \n    {ProfileUtils.getFormattedSize(totalSize)}";
	  	}
	  	else {
	  	    info = 	"Reduce tasks: \n    {numTasks}\n \n"
	  	    		"Total output size: \n    {ProfileUtils.getFormattedSize(totalSize)}";
	  	}
	  	
	  	borderInfo = BorderGroup {
	        	    groupX : bind buttonsX
	        	    groupY : bind buttonsY + 175
	        	    groupWidth : bind buttonWidth
	        	    groupHeight : 95
	        	    groupTitle : "Summary"
	        	    textWidth : 74
	        	}
	  	
	  	infoText = Text {
	        layoutX: bind buttonsX + 10
	        layoutY: bind buttonsY + 200;
	        content: info;
	  	}
    }

	 /**
	  * Create the four buttons on the right
	  */
	function createButtons() {
		// 4 buttons on the right panel
	    buttons = VBox {
	        layoutX: bind buttonsX
	        layoutY: bind buttonsY
	        spacing: 15
	        content : [
	        Button {
              	text : "Map Input"
              	font : COMMON_FONT_14
                layoutInfo: LayoutInfo {
					width: buttonWidth
                }
              	action : function() : Void {
	            	createHistogramChart(0);
    	        }
	           onMouseEntered : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.DEFAULT;
           }
	        },
	        Button {
              	text : "Map Output"
              	font : COMMON_FONT_14
                layoutInfo: LayoutInfo {
					width: buttonWidth
                }
              	action : function() : Void {
               		createHistogramChart(1);
              	}
	           onMouseEntered : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.DEFAULT;
	           }
	        },
	        Button {
              	text : "Reduce Input"
              	font : COMMON_FONT_14
                layoutInfo: LayoutInfo {
                	width: buttonWidth
                }
              	action : function() : Void {
              		createHistogramChart(2);
               	}
	           onMouseEntered : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.DEFAULT;
	           }
	        },
	        Button {
              	text : "Reduce Output"
              	font : COMMON_FONT_14
                layoutInfo: LayoutInfo {
					width: buttonWidth
                }
              	action : function() : Void {
					createHistogramChart(3);
              	}
	           onMouseEntered : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.DEFAULT;
	           }
	        }
	        ]
	    };
	}
	
    /*
	 * create View, the general work includes collecting data, init the nodes, render the layout
	 * 	it is called when populateView() in Window.fx 
	 */
    override function createView() {
        if (not manager.loadProfilesForMRJob(job)) {
            buttons.visible = false;
	        errorText = Text {
				content : "Data Skew informaton is not available.\n"
						  "No job or task profiles were found for job: {job.getExecId()}"
				x : 40
				y : 75
				wrappingWidth: bind width - 40
				font : COMMON_FONT_16
	        }
	        return;
        }
        
        createButtons();
        createHistogramChart(0);
	    (buttons.content[0] as Button).requestFocus();
	    toolTip = ToolTip { };
	}
    
    /////////////////
    // create node //
    /////////////////
    override function create():Node {
       Group {
            content: bind [errorText, barChart, buttons, borderInfo, infoText, toolTip]
       };
    }
}
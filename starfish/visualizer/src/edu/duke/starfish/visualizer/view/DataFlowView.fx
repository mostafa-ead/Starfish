package edu.duke.starfish.visualizer.view;

import javafx.animation.Timeline;
import javafx.animation.KeyFrame;
import javafx.animation.Interpolator;
import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.Cursor;
import javafx.scene.shape.Line;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;
import javafx.scene.shape.Arc;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.control.Button;
import javafx.scene.control.Slider;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ScrollBar;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.KeyCode;
import javafx.scene.paint.Color;
import javafx.scene.text.FontWeight;
import javafx.util.Sequences;
import javafx.util.Math.*;

import java.util.HashMap;

import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.whatif.WhatIfUtils;
import edu.duke.starfish.visualizer.view.custom.ToolTip;
import edu.duke.starfish.visualizer.model.transfers.DataFlowModel;
import edu.duke.starfish.visualizer.model.transfers.HostTransfers;
import edu.duke.starfish.visualizer.view.Constants.*;



/**
 * @author Herodotos Herodotous, Fei Dong, Nick Bognar
 * 
 * DataTransferView.fx
 * 
 * View the data traffic among nodes.
 */

class VertexCheckBox extends CustomNode {
    var hostId: Integer;
    var text: String;
    var font: Font;
    var checkbox : CheckBox;
    
	// Construct the check box
	public override function create(): Node {
		checkbox = CheckBox {
			text : text
			font : font;
			selected : true
			layoutX : bind - horizSB.value
			layoutY : bind  20 * (hostId - round(vertSB.value)) + 15
			visible : bind (20 * (hostId - round(vertSB.value)) + 10) >= 0
					   and (20 * (hostId - round(vertSB.value)) + 10) <= 20 * maxNumNodesInScroll

			onMouseClicked : function(e : MouseEvent) : Void {
				toggleDataVertex();
			}
			
			onKeyReleased : function(k : KeyEvent) : Void {
				if(k.code == KeyCode.VK_SPACE) {
					toggleDataVertex();
				}
			}
		};
		
		return checkbox;
	}
	
	/*
	 * Toggle the visibility of the data vertex
	 */
	function toggleDataVertex(): Void {
	    if (checkbox.selected) {
	    	showDataVertex();	
	    }
	    else {
	    	hideDataVertex();
	    }
	}
	
	/*
	 * Makes a particular data vertex not visible and adjusts the visual ids
	 * of all subsequent vertices
	 */
	function hideDataVertex(): Void {

	    // Make the vertex not visible
	    if (vertices[hostId].visible) {
	        vertices[hostId].visible = false;
	        checkbox.selected = false;
	        --numVisibleVertices;
	        
	        // Decrement the visual id of all subsequent vertices
	        for (pos in [(hostId+1)..(numHosts-1)]) {
	            --vertices[pos].visualId;
	        }
	        
	        if (numVisibleVertices == 0) {
	        	selectAll.selected = false;
	        	selectAll.defined = true;
	        } else {
	            selectAll.defined = false;
	        }
	    }
	}
	
	/*
	 * Makes a particular data vertex visible and adjusts the visual ids
	 * of all subsequent vertices
	 */
	function showDataVertex(): Void {
	    
	    // Make the vertex visible
	    if (not vertices[hostId].visible) {
	        vertices[hostId].visible = true;
	        checkbox.selected = true;
	        ++numVisibleVertices;
	        
	        // Increment the visual id of all subsequent vertices
	        for (pos in [(hostId+1)..(numHosts-1)]) {
	            ++vertices[pos].visualId;
	        }
	        
	        if (numVisibleVertices == numHosts) {
		        selectAll.selected = true;
		        selectAll.defined = true;
	        } else {
	            selectAll.defined = false;
	        }
	    }
	}
	
} // End VertexCheckBox

/**
 * Represents a data vertex in the circle of nodes
 */ 
class DataVertex extends CustomNode {
    
	public var name : String;
	public var hostId : Integer;
	public var visualId : Integer;
	
	public var angle : Number;
	public var centerX : Number;
	public var centerY : Number;
	
	public var displayName : String;
	
	// Construct the vertex
	public override function create(): Node {
	    var length = (hostId + 1).toString().length();
	    var toolTipContent = "Name : {name}";
	    displayName = "{hostId + 1} ({name})";
	    
	    return Group{
	    	content : [
				Circle {
    				centerX : bind centerX
   					centerY : bind centerY
    				radius : 18
    				fill : Color.DARKGREEN.ofTheWay(Color.BLACK, 0.20) as Color
    			}
    			Text {
    			    x : bind centerX - length * 4.5
    			    y : bind centerY + 4.5
    			    font: HOST_ID_FONT
					content: (hostId + 1).toString();
					fill : Color.WHITE
    			}
	    	]

			onMouseMoved : function(e : MouseEvent) : Void {
			    // Enable the toolTip
			    hoverOverHost = true;
			    toolTip.show(toolTipContent, e.x, e.y, 0, 0, 300, 14);
			}
			
			onMouseExited : function(e : MouseEvent) : Void {
			    // Disaple the toolTip
			    hoverOverHost = false;
			    toolTip.hide();
			}
	    };
	}

} // End DataVertex


/**
 * Represents an edge between two hosts (vertices) containing the data transfers
 */ 
class DataEdge extends CustomNode {
    
	public var vertex1 : DataVertex;
	public var vertex2 : DataVertex;
	public var size1 : Long; // From vertex1 to vertex2
	public var size2 : Long; // From vertex2 to vertex1
	public var size1PerInterval : Long[];
	public var size2PerInterval : Long[];
	
	var line: Line;
	var toolTipTotalsContent : String;
	
	// Construct the vertex
	public override function create(): Node {
	    
		toolTipTotalsContent = 	"From : {vertex1.displayName}\n"
							 	"To : {vertex2.displayName}\n"
							 	"Data : {ProfileUtils.getFormattedSize(size1)}\n"
							 	"From : {vertex2.displayName}\n"
							 	"To : {vertex1.displayName}\n"
							 	"Data :  {ProfileUtils.getFormattedSize(size2)}";
	    
		line = Line {
			startX : bind vertex1.centerX
			startY : bind vertex1.centerY
			endX : bind vertex2.centerX
			endY : bind vertex2.centerY
			strokeWidth : bind 	if (showTotalTransfers)
									minEdgeStroke + ((size1 + size2) - minTransfer) / bytesPerPixel
								else
									(size1PerInterval[(currentTime / timePerInterval) as Integer] + 
									 size2PerInterval[(currentTime / timePerInterval) as Integer])
									 / bytesPerPixelInInterval
			stroke : Color.BLUE
			visible : bind vertex1.visible and vertex2.visible and line.strokeWidth > 0
		};
		
		line.onMouseMoved = function(e : MouseEvent) : Void {
		    
		    var toolTipContent : String = toolTipTotalsContent;
		    if (not showTotalTransfers) {
				toolTipContent = 
					"From : {vertex1.displayName}\n"
					"To : {vertex2.displayName}\n"
					"Data : {ProfileUtils.getFormattedSize(size1PerInterval[(currentTime / timePerInterval) as Integer])}\n"
					"From : {vertex2.displayName}\n"
					"To : {vertex1.displayName}\n"
					"Data :  {ProfileUtils.getFormattedSize(size2PerInterval[(currentTime / timePerInterval) as Integer])}";
		    }
		    
			// Enable the toolTip
			line.stroke = Color.ORANGE;
			if (not hoverOverHost)
				toolTip.show(toolTipContent, e.x, e.y, 0, 0, 300, 76);
		};
			
		line.onMouseExited = function(e : MouseEvent) : Void {
		    // Disaple the toolTip
		    line.stroke = Color.BLUE;
		    toolTip.hide();
		};
		
		return line;
	}
} // End DataEdge


/**
 * Represents an arc edge around the same host (vertex) containing
 * the local data transfers
 */ 
class DataArc extends CustomNode {
    
	public var vertex : DataVertex;
	public var size : Long;
	public var sizePerInterval : Long[];
	
	var arc : Arc;
	var toolTipTotalsContent : String;
	
	// Construct the vertex
	public override function create(): Node {
	    
		toolTipTotalsContent = 	"From : {vertex.displayName}\n"
							 	"To : {vertex.displayName}\n"
							 	"Data : {ProfileUtils.getFormattedSize(size)}";
	    
		arc = Arc {
			centerX : bind vertex.centerX + 15 * cos(vertex.angle)
			centerY : bind vertex.centerY - 15 * sin(vertex.angle)
			radiusX : 12
			radiusY : 12
			strokeWidth : bind 	if (showTotalTransfers)
									minEdgeStroke + (size - minTransfer) / bytesPerPixel
								else
									sizePerInterval[(currentTime / timePerInterval) as Integer]
									/ bytesPerPixelInInterval
			startAngle : 0
			length : 360
			fill : Color.WHITE
			stroke : Color.BLUE
			visible : bind vertex.visible and arc.strokeWidth > 0
		};
		
		arc.onMouseMoved = function(e : MouseEvent) : Void {
		    var toolTipContent : String = toolTipTotalsContent;
		    if (not showTotalTransfers) {
				toolTipContent = 
					"From : {vertex.displayName}\n"
					"To : {vertex.displayName}\n"
					"Data : {ProfileUtils.getFormattedSize(sizePerInterval[(currentTime / timePerInterval) as Integer])}";
		    }
		    
		    // Enable the toolTip
		    arc.stroke = Color.ORANGE;
		    if (not hoverOverHost)
		    	toolTip.show(toolTipContent, e.x, e.y, 0, 0, 300, 39);
		}
		
		arc.onMouseExited = function(e : MouseEvent) : Void {
		    // Disaple the toolTip
		    arc.stroke = Color.BLUE;
		    toolTip.hide();
		}
	    
	    return arc;
	}
} // End DataArc


/*
 * The main Dataflow view 
 */
public class DataFlowView extends AppView {
    
    ///////////////////////
    // MODEL VARIABLES   //
    ///////////////////////
    var hosts: String[];
    var numHosts: Integer;
    var model: DataFlowModel;
    
    // Transfers
    var minTransfer : Long;
    var maxTransfer : Long;
    var maxTransferInInterval : Long;
    
    ///////////////////////
    // VIEW VARIABLES    //
    ///////////////////////
	var errorText: Text;
	var toolTip: ToolTip = ToolTip {textWrappingWidth: 300};
	var hoverOverHost: Boolean = false;
	
	// The menu with the filters
    var filterMenu : Node[];
    var showTotalsCheckBox : CheckBox;
    var selectAll : CheckBox;
    var checkHosts : VertexCheckBox[];
    var vertSB : ScrollBar;
    var horizSB : ScrollBar;
    var paneHeight = bind 20 + min(20 * ((height/40) as Integer), 20 * numHosts);
	var maxNumNodesInScroll: Integer = bind ((paneHeight - 20) / 20) as Integer;
	
	// Vertices
	var vertices : DataVertex[];
	var numVisibleVertices : Integer;
	var verticesMap : HashMap;
	
	// Edges and arcs
	var edges : DataEdge[];
	var arcs : DataArc[];
	var minEdgeStroke: Integer = 2;
	var maxEdgeStroke: Integer = 10;
    var bytesPerPixel : Number;
    var bytesPerPixelInInterval : Number;
    
    // The time box
	var timeBox : Node[];
	var timeline: Timeline;
	
    var currentTime : Number;
    var jobDuration : Long;
	var timePerInterval : Number;
	
	var showPlayButton : Boolean = true;
	var showTotalTransfers : Boolean = true;
	def TIMELINE_DURATION : Duration = 12s;
	def NUM_INTERVALS : Integer = 100;
	    
    ///////////////////////
	// CONSTANTS         //
	///////////////////////
	def HOST_ID_FONT : Font = Font.font(Font.DEFAULT.family, FontWeight.BOLD, 14);
	
	var startY = bind max(30, min(40, height / 20));
	var menuWidth = bind width/6 + 55;
	var timeHeight = 100 as Integer;
	var cx = bind (width + menuWidth) / 2;
	var cy = bind (height - timeHeight + startY) / 2;


	/*
	 * Create the menu with the filters
	 */
	function createFilterMenu(): Void {
		
		// Filter title
    	var title = Text {
		    x : 20
		    y : bind startY + 30
			content : "Filters"
			font : COMMON_FONT_18
		};
		
		showTotalsCheckBox = CheckBox {
		    layoutX : 20
		    layoutY : bind startY + 40
			text : "Show Total Transfers"
			font : COMMON_FONT_12
			allowTriState : false
			selected : true
			defined : true
			
			onMouseClicked : function(e : MouseEvent) : Void {
			    showTotalTransfers = showTotalsCheckBox.selected;
			    if (showTotalTransfers) {
					currentTime = 0;
					timeline.pause();
					showPlayButton = true;
			    }
			}
			
			onKeyReleased : function(k : KeyEvent) : Void {
				if(k.code == KeyCode.VK_SPACE) {
			    	showTotalTransfers = showTotalsCheckBox.selected;
				    if (showTotalTransfers) {
						currentTime = 0;
						timeline.pause();
						showPlayButton = true;
				    }
				}
			}
			
		};
		
    	// The selectAll checkbox
		selectAll = CheckBox {
		    layoutX : 20
		    layoutY : bind startY + 65
			text : "Select All / None"
			font : COMMON_FONT_12
			allowTriState : false
			selected : true
			defined : true
			
			onMouseClicked : function(e : MouseEvent) : Void {
			    if (selectAll.selected) {
			        selectAll.defined = true;
			        for(c in checkHosts) c.showDataVertex();
			    } else {
			        selectAll.defined = true;
			        for(c in checkHosts) c.hideDataVertex();
			    }
			}
			
			onKeyReleased : function(k : KeyEvent) : Void {
				if(k.code == KeyCode.VK_SPACE) {
				    if (selectAll.selected) {
				        selectAll.defined = true;
				        for(c in checkHosts) c.showDataVertex();
				    } else {
				        selectAll.defined = true;
				        for(c in checkHosts) c.hideDataVertex();
				    }
				}
			}
			
		};
		
		// The list of checkHosts
		delete checkHosts;
		var pos: Integer = 0;
		for (host in hosts) {
			var checkBox = VertexCheckBox {
			    hostId : pos
				text : "{pos+1}: {host}"
				font : COMMON_FONT_12
			};
			insert checkBox into checkHosts;
			++pos
		}
		
		// Find the (visual) length of the longest host name
		pos = 0;
		var length: Number = 0;
		var maxLength: Number = 0;
		for (host in hosts) {
		    length = Text {content : "{pos+1}: {host}" font : COMMON_FONT_12}.layoutBounds.width; 
		    if (length > maxLength) {
		        maxLength = length;
		    }
		    ++pos;
		}
		
		// The  scrollable pane with the check boxes
    	var pane = Group {
    		layoutX : 35
    		layoutY : bind startY + 75
    		content : [
    			for(c in checkHosts) 
    				c,
	        	Rectangle {
	        	    translateX : -2
	        	    translateY : 8
	        	    width : bind width/6 + 2
	        	    height : bind paneHeight - 8
					fill: Color.WHITE
					opacity : 0.0
					
					onMouseWheelMoved: function(event) {
						vertSB.value += event.wheelRotation
					}
	        	}
	        	Rectangle {
	        	    translateX : -35
	        	    translateY : 8
	        	    width : 33
	        	    height : bind paneHeight - 8
					fill: Color.WHITE
					blocksMouse : true;
	        	}
	        	Rectangle {
	        	    x : bind width/6 - 4
	        	    translateY : 8
	        	    width : 200
	        	    height : bind paneHeight - 8
					fill: Color.WHITE
					blocksMouse : true;
	        	}
    			vertSB = ScrollBar {
    				layoutX : bind width/6
    				height : bind paneHeight
    				min : 0
    				max : bind max(0,(numHosts - maxNumNodesInScroll))
    				value : 0
    				unitIncrement : 1
    				blockIncrement : 1
    				vertical : true
    				focusTraversable : true
    				visible : bind numHosts - maxNumNodesInScroll > 0
    			},
    			horizSB = ScrollBar {
    				layoutY : bind paneHeight
    				width : bind width/6
    				min : 0
    				max : bind max(maxLength - width/6 + 50, 0)
    				value : 0
    				unitIncrement : 15
    				blockIncrement : 15
    				vertical : false
    				focusTraversable : true
    				visible : bind maxLength - width/6 + 50 > 0
    			}
    		]
    	};
    	
	    // Vertical line
		var line = Line {
			startX : bind width/6 + 55
			startY : bind startY
			endX : bind width/6 + 55
			endY : bind height
			strokeWidth : 1
			stroke: Color.SILVER
		}
			
		// Set the filter's menu
		filterMenu = [title, showTotalsCheckBox, selectAll, pane, line];
	}

	/*
	 * Create the data vertices. There exists one vertex for each host
	 */
	function createDataVertices(): Void {
	    delete vertices;
	    var id: Integer = 0;
	    numHosts = hosts.size();
	    numVisibleVertices = numHosts;
	    verticesMap = new HashMap();
	    
    	for(host in hosts) {
    	    
    		var vertex : DataVertex = DataVertex {
    			name : host
    			hostId : id
    			visualId : id
    			angle : bind 2 * PI * vertex.visualId / numVisibleVertices
    			centerX : bind cx + (cx - menuWidth - 50) * cos(vertex.angle)
    			centerY : bind cy - (cy - 70) * sin(vertex.angle)
    		}
    		
    		insert vertex into vertices;
    		++id;
    		verticesMap.put(host, vertex);
    	} 
	}
	
	/*
	 * Create the data edges. There exists one edge between two hosts that
	 * exchanged data, or an arc around a host representing local transfers
	 */
	function createDataEdges(): Void {
	    delete edges;
	    delete arcs;
	    for(transfer in model.getHostTransfers()) {
	        if (transfer.getHost1().equals(transfer.getHost2())) {
	            // Create an arc edge around a single vertex
		        var arc = DataArc {
		            vertex : verticesMap.get(transfer.getHost1()) as DataVertex
		            size : transfer.getTotalData1()
		            sizePerInterval : transfer.getData1PerInterval();
		        }
		        insert arc into arcs;
	        } else {
	            // Create an edge between two vertices
		        var edge = DataEdge {
		            vertex1 : verticesMap.get(transfer.getHost1()) as DataVertex
		            vertex2 : verticesMap.get(transfer.getHost2()) as DataVertex
		            size1 : transfer.getTotalData1()
		            size2 : transfer.getTotalData2()
		            size1PerInterval : transfer.getData1PerInterval();
		            size2PerInterval : transfer.getData2PerInterval();
		        }
		        insert edge into edges;
	        }
	    }
	    
	}
	
	function createTimelineBox(): Void {

		// Create a timeline with the underlying playing functionality
		currentTime = 0;
	    timeline = Timeline {
    		keyFrames : [
    			KeyFrame {
    				time : TIMELINE_DURATION
    				values : currentTime => jobDuration tween Interpolator.LINEAR;
    			}

    			KeyFrame {
    			    // Resets the timeline at the end
    				time : TIMELINE_DURATION + 1ms
    				action : function() : Void {
    					currentTime = 0;
    					showPlayButton = true;
    					showTotalTransfers = true;
    					showTotalsCheckBox.selected = true;
    				}
    			}
    		]
	    };
	    
		// Play button
		var playButton = Button {
			text : "PLAY"
			layoutX : bind menuWidth + 18
			layoutY : bind height - 75
			width : 50
			visible : bind showPlayButton
			
			action : function() : Void {
				timeline.time = currentTime / jobDuration * TIMELINE_DURATION;
				timeline.play();
				showPlayButton = false;
				showTotalTransfers = false;
				showTotalsCheckBox.selected = false;
			}

			onMouseEntered : function(e : MouseEvent) : Void {
				this.scene.cursor = Cursor.HAND;
			}
			
			onMouseExited : function(e : MouseEvent) : Void {
				this.scene.cursor = Cursor.DEFAULT;
			}
		};
		
		// Pause putton
		var pauseButton = Button {
			text : "PAUSE"
			layoutX : bind menuWidth + 18
			layoutY : bind height - 75
			width : 50
			visible : bind (not showPlayButton)
			
			action : function() : Void {
				timeline.pause();
				showPlayButton = true;
			}

			onMouseEntered : function(e : MouseEvent) : Void {
				this.scene.cursor = Cursor.HAND;
			}
			
			onMouseExited : function(e : MouseEvent) : Void {
				this.scene.cursor = Cursor.DEFAULT;
			}
		}
		
		// Slider
		var slider = Slider {
			layoutX : bind menuWidth + 90
			layoutY : bind height - 70
			width : bind width - menuWidth - 220
			min : 0
			max : bind jobDuration
			value : bind currentTime with inverse
			clickToPosition : true
			focusTraversable : true
			
			onMouseDragged : function(e: MouseEvent) {
				showTotalTransfers = false;
				showTotalsCheckBox.selected = false;
			}
			
			onMousePressed : function(e: MouseEvent) {
				showTotalTransfers = false;
				showTotalsCheckBox.selected = false;
			}
		};
		
		// Current time
		var timeText = Text {
			content : bind 	if (showTotalTransfers) 
								ProfileUtils.getFormattedDuration(jobDuration as Long)
							else
								ProfileUtils.getFormattedDuration(currentTime as Long)
			x : bind width - 120
			y : bind height - 60
		}
		
		// Horizontal line
		var line = Line {
			startX : bind width/6 + 55
			startY : bind height - timeHeight
			endX : bind width
			endY : bind height - timeHeight
			strokeWidth : 1
			stroke: Color.SILVER
		}
		
		// Set the contents of the time box
		timeBox = [slider, playButton, pauseButton, timeText, line];
	}

    /*
	 * Create the entire view
	 */
	override function createView(): Void {
	    
	    var success: Boolean = manager.loadDataTransfersForMRJob(job);
	    if ((not success) and manager.loadProfilesForMRJob(job)) {
	        var conf = manager.getHadoopConfiguration(job.getExecId());
	        success = WhatIfUtils.generateDataTransfers(job, conf);
	    }
	    
	    if(not success) {
	        errorText = Text {
				content : "Data Flow informaton is not available.\n"
						  "No job or task profiles were found for job: {job.getExecId()}"
				x : 40
				y : 75
				wrappingWidth: bind width - 40
				font : COMMON_FONT_16
	        }
	        return;
	    }
	    
	    // Get all the host names and sort them
	    delete hosts;
	    var cluster = manager.getClusterConfiguration(job.getExecId());
	    for(tracker in cluster.getAllTaskTrackersInfos()) {
	        insert tracker.getHostName() into hosts;
	    }
	    hosts = Sequences.sort(hosts) as String[];

		// Create the model
		model = new DataFlowModel(job, NUM_INTERVALS);
		
		// Set the transfer-related parameters
		minTransfer = model.getMinTransfer();
		maxTransfer = model.getMaxTransfer();
		maxTransferInInterval = model.getMaxTransferInInterval();
		if (minTransfer == maxTransfer) {
		    minTransfer -= 4;
		    maxTransfer += 4;
		}
		bytesPerPixel = ((maxTransfer - minTransfer) as Number) / (maxEdgeStroke - minEdgeStroke);
		bytesPerPixelInInterval = (maxTransferInInterval as Number) / (maxEdgeStroke - minEdgeStroke);
		
		// Set the job timings
		jobDuration = job.getEndTime().getTime() - job.getStartTime().getTime();
		timePerInterval = (jobDuration as Number) / NUM_INTERVALS;
						
		// Create the view	    	
	    createFilterMenu();
	    createDataVertices();
	    createDataEdges();
	    createTimelineBox();
	}
	
    /////////////////
	// create node //
	/////////////////	
    override function create():Node {
		Group {
            content: bind [
            	Group { content : bind filterMenu},
            	Group { content : bind arcs},
            	Group { content : bind edges},
            	Group { content : bind vertices},
            	timeBox,
            	toolTip,
         		errorText
			];
       	};    
    }
}

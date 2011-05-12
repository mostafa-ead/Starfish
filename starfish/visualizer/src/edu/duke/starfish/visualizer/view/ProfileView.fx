package edu.duke.starfish.visualizer.view;

import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.shape.Line;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.text.TextAlignment;
import javafx.scene.control.Button;
import javafx.scene.Cursor;
import javafx.stage.Stage;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.paint.LinearGradient;
import javafx.scene.paint.Stop;
import javafx.scene.text.FontWeight;
import javafx.scene.input.MouseEvent;
import javafx.util.Math;
import javafx.scene.transform.Transform;
import javafx.scene.transform.Rotate;
import javafx.ext.swing.SwingComponent;

import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.DefaultTableCellRenderer;

import java.util.EnumMap;
import java.util.List;
import java.text.NumberFormat;

import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRJobProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.MRTaskProfile;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRTaskPhase;
import edu.duke.starfish.visualizer.view.Constants.*;
import edu.duke.starfish.visualizer.view.custom.BorderGroup;
import edu.duke.starfish.visualizer.view.custom.ToolTip;
import edu.duke.starfish.visualizer.model.ReadOnlyTableModel;



/**
 * @author Fei Dong (dongfei@cs.duke.edu), Herodotos Herodotou
 * 
 * ProfileView.fx
 * 
 * places sub phases into a stack and lists some core counters and statistics.
 */

 /////////////////////////
 //// Table Related //////
 /////////////////////////
 public class TableColumns {
 	var content : String[];
 }
 
 public class TableCell {
 	var text : String;
 }
 
 public class TableRow {
 	var cells : TableCell[];
 }
 
 // A swing table
 public class TableComponent extends SwingComponent {
 	var table : JTable;
 	var model : ReadOnlyTableModel;
 	var selectionNum : Integer;

 	var columns : TableColumns on replace{
  		model = new ReadOnlyTableModel(columns.content, 0);
 		table.setModel(model);
 	};
 	
	
 	var rows : TableRow[] on replace oldValue[lo..hi] = newVals {
 		for(index in [hi..lo step -1]){
 			model.removeRow(index);
 		}
 
 		for(row in newVals){
 			model.addRow(for(cell in row.cells) cell.text);
 		}
 	};
 
 	// Constructor
 	override function createJComponent() {
 		table = new JTable(new ReadOnlyTableModel());
 		model = table.getModel() as ReadOnlyTableModel;
 		table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
 
 		table.getSelectionModel().addListSelectionListener(javax.swing.event.ListSelectionListener {
 			override function valueChanged(e : javax.swing.event.ListSelectionEvent) {
 				selectionNum = table.getSelectedRow();
 			}
 		});
 		return new javax.swing.JScrollPane(table);
 	}
 	
 	// Fix the width of the column
 	function fixColumnWidth(col: Integer, width: Number): Void {
		var colModel = table.getColumnModel().getColumn(col);
        colModel.setPreferredWidth(width);
 	}
 	
 	// Center all text in a column
 	function alignColumn(col: Integer, align: Integer): Void {
		var colModel = table.getColumnModel().getColumn(col);
 		var dtcr = new DefaultTableCellRenderer();    
 		dtcr.setHorizontalAlignment(align);  
 		colModel.setCellRenderer(dtcr);
 	}
}
 
 
 // A table with profile information
public class ProfileTable extends CustomNode {
	var startX : Number;
	var startY : Number;
 	var width : Number;
 	var height : Number;
 	
 	var title: String;
 	var profileKeys: String[];
 	var profileValues: String[];
 	
 	var titleWidth: Number; // Temp until migrate to JavaFX 1.3
 	
 	public override function create(): Node {
 	    
        // The border with the title
        var border = BorderGroup {
        	    groupX : bind startX
        	    groupY : bind startY
        	    groupWidth : bind width
        	    groupHeight : bind height
        	    groupTitle : title
        	    textWidth : titleWidth
        	};
        
        // The actual table
        var table = TableComponent{
  	  	 	 	columns : TableColumns {
     	  	 	 		content : ["Profile Field", "Value"]
   	  	 	 	};

   	  	  		rows: bind [ for (v in profileValues) TableRow {
     	  	 			cells: [
     	  	 				TableCell {text : profileKeys[indexof v]}
     	  	 				TableCell {text : v} 		
     	  	 				]
	   	  	 			}
  	   	  	  	];
            	
            	layoutX: bind border.groupX + 10
            	layoutY: bind border.groupY + 10
            	width: bind border.groupWidth - 20
            	height: bind border.groupHeight - 20
            };
        
        table.fixColumnWidth(0, 300);
        table.alignColumn(1, SwingConstants.RIGHT);
        
       return Group {
	        content : [ border, table ]
        };   
 	}
 	
}


/*
 *  A customr stacked chart
 */
public class CustomChart  extends CustomNode {
    
    var start_x: Number;
    var start_y: Number;
    var end_y: Number;
    var unit: Integer;
    var pixsPerSec: Number;
    var numTicks: Integer;
    
    var title: String;
    var phases: MRTaskPhase[];
    var timings: Number[];
    var counts: Number[];
    
    var toolTipWidths: Number[]; // TEMP until we migrate JavaFX 1.3
    var toolTipHeights: Number[]; // TEMP until we migrate JavaFX 1.3

    var isMap: Boolean;

    // Constructor
    public override function create(): Node {

		// Create the graph
		var graph = Group{ content: [] };

		// chart title
        insert Text {
            	x : start_x - 20 ;
            	y : end_y - 60;
            	font: Font.font(Font.DEFAULT.family, FontWeight.BOLD, 16)
            	content : title
            	wrappingWidth: 200
            	textAlignment: TextAlignment.CENTER
		} into graph.content;
		
      	// x axes
		insert Line {
       			startX : start_x
       			startY : bind start_y + 2
       			endX : start_x + 100 
       			endY : bind start_y + 2
       			strokeWidth : 3
       	} into graph.content;
       	
       	// y axes
		insert Line {
      	       	startX : start_x
      	       	startY : bind start_y + 2
      	       	endX : start_x
      	       	endY : bind end_y - 20
      	       	strokeWidth : 3
      	} into graph.content;
      	
      	// tick mark on y axes
      	var label_x = start_x;
 		for (i in [0..numTicks]) {
 			insert Line {
 					startX : start_x 
 					startY : bind start_y + (pixsPerSec*unit*i)
 					endX : start_x - 4
 					endY : bind start_y + (pixsPerSec*unit*i)
 					strokeWidth : 1
 					visible: bind (start_y + (pixsPerSec*unit*i) >= end_y - 18)
 			} into graph.content;
 			
          	var text = Text {
          			y : bind start_y + (pixsPerSec*unit*i) + 3;
          			font : COMMON_FONT_12
          			content : (unit*i).toString()
          			visible: bind (start_y + (pixsPerSec*unit*i) >= end_y - 18)
          	}
          	text.x = start_x - text.layoutBounds.width - 8;
          	label_x = text.x;
          	insert text into graph.content;
 		}
 		
   	    // y axes title
   		var yAxesTitle = Text {
   	       		x : label_x - 10;
   	       		y : bind (start_y + end_y) / 2 + 20
   	       		//rotate : -90
   	      		transforms:  [ Rotate { 
   	      			angle: -90 
   	      			pivotX : label_x - 10 
   	      			pivotY : bind (start_y + end_y) / 2 + 20 }];
   	       		font : COMMON_FONT_14
   	     		content : "Running Time (sec)"
        }
        insert yAxesTitle into graph.content;
		
 		// stackable columns
        for (i in [0..<counts.size()]) {
             var color = Color.web(colors[i+19]);
             if (isMap) {
                color = Color.web(colors[i+9]);
             }
             
             insert Rectangle {
                x: start_x + 30
                y: bind start_y + counts[i]*pixsPerSec
                width: 40
                height: bind -(timings[i]*pixsPerSec)
                fill: createGradient(color)
                strokeWidth: 1.0
                stroke: Color.BLACK;
                
				onMouseMoved: function(e: MouseEvent): Void {
				    // Show the toolTip
				    if (timings[i] < 1) {
				    	toolTip.show("{phases[i].getName()}: {(timings[i] * 1000) as Integer} ms    ", e.sceneX, e.sceneY, 0, 0, 120, 13);
				    } else {
				    	toolTip.show("{phases[i].getName()}: {timings[i] as Integer} sec   ", e.sceneX, e.sceneY, 0, 0, 120, 13);
				    }
				}
				
				onMouseExited : function(e : MouseEvent) {
				    // Hid the toolTip
				    toolTip.hide();
				}
                
             } into graph.content;
        }
        
        // legend
        for (i in [0..<counts.size()]) {
             var color = Color.web(colors[i+19]);
             if (isMap) {
                color = Color.web(colors[i+9]);
             }
 
            insert Group {
            	content: [
			        Rectangle
			        {
		                x : start_x + 88;
		                y : bind start_y - 30*i - 102;
			            width: 80
			            height: 16,
			            fill: Color.WHITE,
			        }
			        Rectangle
			        {
		                x : start_x + 90;
		                y : bind start_y - 30*i - 100;
			            width: 12
			            height:  12,
			            fill: createGradient(color),
			            strokeWidth: 1.0
			            stroke: Color.BLACK;
			        }
			 		Text {
		                x : start_x + 108;
		                y : bind start_y - 30*i - 88;
			  			font : COMMON_FONT_14
			  			content : phases[i].getName();
			  		}
            	]
            	
				onMouseMoved: function(e: MouseEvent): Void {
				    // Show the toolTip
				   	toolTip.show(phases[i].getDescription(), e.sceneX, e.sceneY, 0, 0, toolTipWidths[i], toolTipHeights[i]);
				}
				
				onMouseExited : function(e : MouseEvent) {
				    // Hide the toolTip
				    toolTip.hide();
				}
            } into graph.content;
		}
		
        return graph;
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
    
}


/*
 * The main ProfileView
 */
public class ProfileView extends AppView {
 
    ///////////////////////
    // MODEL VARIABLES   //
    ///////////////////////
    var profile: MRJobProfile;
    var avgMapProfileList : List;
    var avgRedProfile: MRTaskProfile;
    var isShowTable: Boolean;
    
    ///////////////////////
    // VIEW VARIABLES    //
    ///////////////////////
    var errorText: Text;
    var toolTip: ToolTip = ToolTip { };

    var viewStartX = bind Math.max(20, 0.1 * width);
    var viewStartY = bind Math.max(30, Math.min(40, height / 20)) + 10;
    	
    // Profile tables view
	var profileTablesView: VBox;
	var tablesWidth = bind width - 2 * viewStartX;
	var tablesHeight = bind height - viewStartY - 90;
	var tablesBuffer = 5;
	
	// Profile graphs view
	var profileTimesView: HBox;

	// Change button
	var changeButton: Button;
	
	/**
	 * Create the view with the phase timings bar graphs
	 */
    function createProfileTimesView(): Void {
        
        var profileTimes: CustomChart[];

        // Get the number of profiles
        var numProfs = avgMapProfileList.size();
        if (avgRedProfile != null) {
            numProfs = numProfs + 1;
        }

        // Populate the map profile tables
        var index = 0;
		for (mapProfile in avgMapProfileList) {
		    var title = if (avgMapProfileList.size() > 1) { 
		    				"Representative\nMap Phase Timings\n (Input Index = {index})"
		    			} else {
		    			    "Representative\nMap Phase Timings"
		    			};
        	
			insert createBarChart(mapProfile as MRTaskProfile, title, true) into profileTimes;
			index += 1;
		}
		
		// Populate the reduce profile table
		if (avgRedProfile != null) {
		    var title = "Representative\nReduce Phase Timings";
		    insert createBarChart(avgRedProfile, title, false) into profileTimes;
		}
		
		// Calculate the total width of the plots
		var plotsWidth = 0.0;
		for (plot in profileTimes) {
		    plotsWidth += plot.layoutBounds.width
		}
		
		// Create the view
	 	profileTimesView = HBox {
	         layoutX: 0
	         layoutY: bind viewStartY
	   	     spacing: bind Math.max(10, (width - plotsWidth) / (numProfs + 1) - 15)
	   	     content: []
	       	 visible: true;
	    }
	    
	    // Insert the plots in the view. The empty groups are for centering the plots
	    insert Group{} into profileTimesView.content;
     	for (plot in profileTimes){
     	    insert plot into profileTimesView.content;
     	}
	    insert Group{} into profileTimesView.content;
	    
    }

	/*
	 * Draw a stackable chart by position. 
	 */
    function createBarChart(profile: MRTaskProfile, title: String, isMap: Boolean): CustomChart {
        
        var phases: MRTaskPhase[];
        var timings: Double[];
        
		// Get the phase timings
		for (entry in profile.getTimings().entrySet()) {
		   insert entry.getKey() into phases;
		   insert entry.getValue()/1000 into timings;
		}
        		
        var start_x = 0;
	    var start_y: Number = bind height;
	    var end_y : Number = bind viewStartY + 170;
       
	    var totalHeight: Number = bind end_y - start_y;
	    var totalTime = 0.0;
	    for (num in timings) {
	   		totalTime += num;
	    }

       // get the round number for unit
       var unit: Integer = Math.round(totalTime/10);
       if (unit >= 100) {
           unit = Math.round(unit/100) * 100;
       } else if (unit >= 10){
           unit = Math.round(unit/10) * 10;
       } else if (unit == 0) {
           unit = 1;
       }
       var numTicks = (Math.ceil(totalTime / unit) + 1) as Integer;
       
       // Calculate the aggregated timings
       var counts : Double[] = [];
    
       for (i in [0..< phases.size()]) {
          insert 0.0 into counts;
       }
       var oldValue: Double = 0.0;
       for (i in [0..< timings.size()]) {
          counts[i] += timings[i] + oldValue;
          oldValue = counts[i];
       }
       
       // Create the chart
	   return CustomChart {
		    start_x: start_x;
		    start_y: bind start_y;
		    end_y: bind end_y;
		    unit: unit;
		    pixsPerSec: bind totalHeight/totalTime;
		    numTicks: numTicks;
		    
		    title: title;
		    phases: phases;
		    timings: timings;
		    counts: counts;
		    
		    // TEMP until we migrate to JavaFX 1.3
		    toolTipWidths: if (isMap) [220, 220, 220, 220, 220, 220, 220] else [220, 220, 220, 220, 220, 220];
		    toolTipHeights:if (isMap) [28, 42, 28, 42, 42, 28, 28] else [42, 28, 28, 28, 28, 28];
		    isMap: isMap;
		};
    }
	
	/*
	 * Create the view with the profile tables
	 */
    function createProfileTablesView(): Void {
        
        var profileTables: ProfileTable[];

        // Get the number of profiles
        var numProfs = avgMapProfileList.size();
        if (avgRedProfile != null) {
            numProfs = numProfs + 1;
        }

        // Populate the map profile tables
        var index = 0;
		for (mapProfile in avgMapProfileList) {
		    var title: String;
		    var titleWidth: Number;
		    if (avgMapProfileList.size() > 1) { 
		    	title = "Representative Map Task Profile (Input Index = {index})";
		    	titleWidth = 392.1;
			} else {
		    	title = "Representative Map Task Profile";
		    	titleWidth = 254.7;
		    };
        	
			insert createProfileTable(mapProfile as MRTaskProfile, title, titleWidth, numProfs) into profileTables;
			index += 1;
		}
		
		// Populate the reduce profile table
		if (avgRedProfile != null) {
		    var title = "Representative Reduce Task Profile";
		    insert createProfileTable(avgRedProfile, title, 281.7, numProfs) into profileTables;
		}
		
		// Create the view
	 	profileTablesView = VBox {
	         layoutX: bind viewStartX
	         layoutY: bind viewStartY
	   	     spacing: tablesBuffer
	   	     content: bind [
	   	     	for (table in profileTables){
	   	     	    table
	   	     	}
	   	     ]
	       	 visible: false;
	    }
    }
    
    
    /*
     *  Create a single profile table
     */
    function createProfileTable(profile: MRTaskProfile, title: String, titleWidth: Number, numTables: Integer): ProfileTable {
        
        var profileKeys: String[];
        var profileValues: String[];
        
        var nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(3);
		nf.setMinimumFractionDigits(0);
		
		// Insert counters
		for (entry in profile.getCounters().entrySet()) {
		   insert entry.getKey().getDescription() into profileKeys;
		   insert nf.format(entry.getValue()) into profileValues;
		}

		// Insert stats
		nf.setMinimumFractionDigits(3);
		for (entry in profile.getStatistics().entrySet()) {
		   insert entry.getKey().getDescription() into profileKeys;
		   insert nf.format(entry.getValue()) into profileValues;
		}
        		
        // Create and return the profile table
        return ProfileTable {
        	startX : bind viewStartX;
        	startY : bind viewStartY;
         	width : bind tablesWidth;
         	height : bind (tablesHeight - (numTables * 2 * tablesBuffer)) / numTables;
         	
         	title: title;
         	titleWidth : titleWidth;
         	profileKeys: profileKeys;
         	profileValues: profileValues;
        }
    }
    
    /*
     *  Create the change button
     */
    function createChangeButton(): Void {
	 	changeButton = Button {
	         text : bind if(isShowTable) "View Detail Profiles" else "View Profile Time",
	         font : COMMON_FONT_14
	         layoutX : viewStartX
	         layoutY : bind height - 70
	         action : function() : Void {
	           	if (isShowTable) {
	 	      		profileTablesView.visible = true;
	 	      		profileTimesView.visible = false;
	           	} else {
	 	      		profileTablesView.visible = false;
	 	          	profileTimesView.visible = true;
	           	}
	           	isShowTable = not isShowTable;
	         }
	           onMouseEntered : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               this.scene.cursor = Cursor.DEFAULT;
	           }
	 	}
    }
    
    /*
	 * create View, the general work includes collecting data, init the nodes, render the layout
	 * 	it is called when populateView() in Window.fx 
	 */
	override function createView() {
	    if (not manager.loadProfilesForMRJob(job)) {
	       	 profileTablesView.visible = false;
	       	 profileTimesView.visible = false;
	       	 changeButton.visible = false;
	         errorText = Text {
				content : "Job Profile informaton is not available.\n"
						  "No job or task profiles were found for job: {job.getExecId()}"
				x : 40
				y : 75
				wrappingWidth: bind width - 40
				font : COMMON_FONT_16
	        }
	        return;
	    }
	    
	    // Get the data
	    isShowTable =  true;
	    profile = job.getProfile();
	    avgMapProfileList = profile.getAvgMapProfiles();
	    avgRedProfile = profile.getAvgReduceProfile();
	    
	    // Create the views
	    createProfileTimesView();
	    createProfileTablesView();
	    createChangeButton();
	}
	
    /////////////////
    // create node //
    /////////////////
	override function create():Node {
        Group {
           content: bind [errorText, profileTablesView, profileTimesView, changeButton, toolTip];
        }      
    }
}

package edu.duke.starfish.visualizer.view;

import javafx.stage.Stage;
import javafx.stage.Alert;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.control.Button;
import javafx.util.Math;
import javafx.scene.input.MouseEvent;
import javafx.scene.Cursor;
import javafx.ext.swing.SwingComponent;
import javafx.lang.FX;

import java.io.PrintStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.lang.Exception;

import javax.swing.JTable;
import javax.swing.JScrollPane;
import javax.swing.JFileChooser;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.DefaultTableCellRenderer;

import edu.duke.starfish.visualizer.model.FileNameExtensionFilter;
import edu.duke.starfish.visualizer.model.WindowType;
import edu.duke.starfish.visualizer.model.AppViewType;
import edu.duke.starfish.visualizer.model.settings.ConfigTableModel;
import edu.duke.starfish.visualizer.model.settings.SpecsTableModel;
import edu.duke.starfish.visualizer.model.settings.SettingsTableType;
import edu.duke.starfish.visualizer.view.custom.BorderGroup;
import edu.duke.starfish.visualizer.view.Constants.*;
import edu.duke.starfish.profile.profileinfo.setup.TaskTrackerInfo;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.DataLocality;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.ClusterConfiguration;
import edu.duke.starfish.profile.profileinfo.utils.Constants.*;
import edu.duke.starfish.profile.profileinfo.utils.ProfileUtils;
import edu.duke.starfish.profile.profileinfo.utils.XMLClusterParser;
import edu.duke.starfish.whatif.data.XMLInputSpecsParser;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import edu.duke.starfish.whatif.WhatIfUtils;
import edu.duke.starfish.whatif.VirtualMRJobManager;
import edu.duke.starfish.jobopt.OptimizedJobManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


/**
 * @author Fei Dong (dongfei@cs.duke.edu), Herodotos Herodotou
 * 
 * SpecificationView.fx
 * 
 * List Hadoop Config, Cluster config and Input specs. It supports normal mode and what-if mode. 
 * * Normal mode, it only show those fields without editing. 
 * * What-if mode, the user can change the values and click what-if button to generate other tabs
 */
/////////////////////////
//// Table Related //////
/////////////////////////
 
 /**
  * ConfigTable is used for create ConfigForm and ClusterForm.
  * This is suitable for Key->Value table
  */
public class ConfigTable extends SwingComponent {
 	var table : JTable;
 	var model : DefaultTableModel;
 	var selectionNum : Integer = -1;
 	var readOnly : Boolean;
 	var type : SettingsTableType;
 	
 	// Assumption the configkeys and configValues are the same size
 	
 	// Config keys never change
 	var configKeys: String[];
 	
 	// if data grid changes, it will trigger the listener and call tableChanged()
 	var configValues: Object[] on replace{
 	 		table.setModel(new ConfigTableModel(configKeys, configValues, readOnly));
 	 		fixColumnWidth(0, 300);
 	 		centerColumn(1);
 	 		
 	 		table.getModel().addTableModelListener(javax.swing.event.TableModelListener{
 	 		override function tableChanged(e: javax.swing.event.TableModelEvent) {
 	 		        var row = e.getFirstRow();
 	 		        var column = e.getColumn();
 	 		        var model = e.getSource() as ConfigTableModel;
 	 		        var data = model.getValueAt(row, column);
 	 		        updateData(type, row, column, data);
 	 		    }
 	 		});
 	 };

 	// construct
 	override function createJComponent() {
 		// model = table.getModel() as javax.swing.table.DefaultTableModel;
 		table = new JTable();
 		table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
 		table.setModel(new ConfigTableModel(configKeys, configValues, readOnly));
 		 		
 		// get select row
 		table.getSelectionModel().addListSelectionListener(javax.swing.event.ListSelectionListener {
 			override function valueChanged(e : javax.swing.event.ListSelectionEvent) {
 				selectionNum = table.getSelectedRow();
 			}
 		});
 		
 		// bind with scroll pane
 		return new JScrollPane(table);
 	}
 	
 	// Fix the width of the column
 	function fixColumnWidth(col: Integer, width: Number): Void {
		var colModel = table.getColumnModel().getColumn(col);
        colModel.setPreferredWidth(width);
 	}
 	        
 	
 	// Center all text in a column
 	function centerColumn(col: Integer): Void {
		var colModel = table.getColumnModel().getColumn(col);
 		var dtcr = new DefaultTableCellRenderer();    
 		dtcr.setHorizontalAlignment(SwingConstants.CENTER);  
 		colModel.setCellRenderer(dtcr);
 	}
}


 /**
  * SpecTable is used for create SpecForm.
  *  This is suitable for many row list table
  */
public class SpecTable extends SwingComponent {
  	var table : JTable;
  	var model : DefaultTableModel;
  	var selectionNum : Integer = -1; // if not selected, -1 is default
  	var readOnly : Boolean;
  	var type : SettingsTableType;

  	// specs is the data source, which is list of MRJobInfo and set the model at the first time
 	var specs: List;
  	 
  	// [Trick] The specs on-replace does not trigger when data changes, but the specSize does change. 
  	// So set the new model when specSize changes
  	// I think it is due to specs is Java List type which is not supported in natual in JavaFX; 
  	// 		if using sequence, the problem may solve.
  	var specSize: Number on replace{
  	    if (specs != null) {
  	        table.setModel(new SpecsTableModel(specs, readOnly));
  	 		table.getModel().addTableModelListener(javax.swing.event.TableModelListener{
  	 		    
  	        	override function tableChanged(e: javax.swing.event.TableModelEvent) {
  	          	 	var row = e.getFirstRow();
  	          	 	var column = e.getColumn();
  	          	 	var model = e.getSource() as SpecsTableModel;
  	          	 	var data = model.getValueAt(row, column);
  	          	 	updateData(type, row, column, data);
  	          	 }
  	        });
  	    }
  	}
  	
	// construct
  	override function createJComponent() {
  		table = new JTable();
  		table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
 		
  		table.getSelectionModel().addListSelectionListener(javax.swing.event.ListSelectionListener {
  			override function valueChanged(e : javax.swing.event.ListSelectionEvent) {
  				selectionNum = table.getSelectedRow();
  			}
  		});
  		
 		// bind with scroll pane
  		return new JScrollPane(table);
  	}
}

/**
 * An export button to export the configuration as XML files
 */
public class ImportButton extends CustomNode {

	var type : SettingsTableType;

    public override function create(): Node {
		return Button {
           text : "Import"
           font : COMMON_FONT_14
           width : 80
           height : 20
           
           action : function() : Void {
				// Ask the user for a file to save to
				var chooser = new JFileChooser();
				var filter = new FileNameExtensionFilter("XML files", "xml");
				chooser.setFileFilter(filter);
				chooser.setCurrentDirectory(new File(importDir));
				var returnVal = chooser.showOpenDialog(null);
				
				// Import the file
				if(returnVal == JFileChooser.APPROVE_OPTION) {
				    importXMLFile(type, chooser.getSelectedFile());
				}
           }
           
           onMouseEntered : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.HAND;
           }
           
           onMouseExited : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.DEFAULT;
           }
           
		}
    }
}
/**
 * An export button to export the configuration as XML files
 */
public class ExportButton extends CustomNode {

	var type : SettingsTableType;
	var defaultFile : String;

    public override function create(): Node {
		return Button {
           text : "Export"
           font : COMMON_FONT_14
           width : 80
           height : 20
           
           action : function() : Void {
				// Ask the user for a file to save to
				var chooser = new JFileChooser();
				var filter = new FileNameExtensionFilter("XML files", "xml");
				chooser.setFileFilter(filter);
				chooser.setSelectedFile(new File(exportDir, defaultFile));
				var returnVal = chooser.showOpenDialog(null);

				// Export the file
				if(returnVal == JFileChooser.APPROVE_OPTION) {
				    exportXMLFile(type, chooser.getSelectedFile());
				}
           }
           
           onMouseEntered : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.HAND;
           }
           
           onMouseExited : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.DEFAULT;
           }
           
		}
    }
}
 
/**
 * The main Setting View
 */
public class SettingsView extends AppView {
    
    public var parentWindow : Window;
    var exportDir : String = FX.getProperty("javafx.user.dir");
    var importDir : String = FX.getProperty("javafx.user.dir");
	
    ///////////////////////
    // Model's variables //
    ///////////////////////
    
    // Conf parameter variables
    var conf: Configuration;
    var confParamKeys : String[];
    var confParamValues : Object[];
    var updateConfParams : Boolean;
    
    // Keep track the indexes of each type of the conf values
    var confParamIntValues : Integer[];
    var confParamFloatValues : Integer[];
    var confParamBoolValues : Integer[];
    
    // Cluster variables
    var cluster : ClusterConfiguration;
    var clusterKeys : String[];
    var clusterValues : Integer[];
    var updateCluster : Boolean;

	// Input specs variables
    var specs: List;
    var specSize: Integer;
    
    //////////////////////
    // View's variables //
    //////////////////////
    var confParamForm: Group;
    var clusterForm: Group;
	var inputSpecsForm: Group;
    var whatIfButton: Button;
    var errorText: Text;

	// Spacing variables
	var startX = bind Math.max(20, 0.1 * width);
	var startY = bind Math.max(30, Math.min(40, height / 20)) + 15;
	var groupsBuffer = 5;
	var groupsWidth = bind width - 2 * startX;
	var groupsHeight = bind height - startY - 3 * groupsBuffer - 90;
	    
    ////////////////////////////
    // DATA FUNCTIONS         //
    ////////////////////////////        
    
    /*
     * Update member variables when data grid changes
     * 
	 * @param type
	 *            type of the form
	 * @param row
	 *            changed row 
	 * @param col
	 *            changed column
	 * @param data 
	 * 			  changed data
     */
    public function updateData(type: SettingsTableType, row: Integer, col: Integer, data: Object):Void {
    
    	if (type == SettingsTableType.CONF_PARAMS) {
   	        confParamValues[row] = data ;
   	        updateConfParams = true;
   	        
    	} else if (type == SettingsTableType.CLUSTER_SPECS) {
    	    clusterValues[row] = data as Integer;
    	    updateCluster = true;
    	    
    	} else if (type == SettingsTableType.INPUT_SPECS) {
    	    // need to set back to MapInputSpecs
    	    var spec = specs.get(row) as MapInputSpecs;
    	    
			if (col == 0) {
    	    	spec.setInputIndex(data as Integer);
			}
			else if (col == 1) {
    	     	spec.setNumSplits(data as Integer);
    	    }
    	    else if (col == 2) {
    	    	spec.setSize((data as Long) * 1024 * 1024);
    	    }
    	    else if (col == 3) {
    	    	spec.setCompressed(data as Boolean);
    	    }
    	}
    }
    
    
    /*
     * Init variables for top 2 tables
     */
    function initData(): Void {
        
        // Initialize the data for the configuration parameters
        confParamKeys = [
				MR_SORT_MB,
				MR_SORT_FACTOR,
				MR_SPILL_PERC, 
	 			MR_SORT_REC_PERC,
				MR_NUM_SPILLS_COMBINE,
				
				MR_RED_TASKS,
				MR_INMEM_MERGE,
				MR_SHUFFLE_MERGE_PERC,
				MR_SHUFFLE_IN_BUFF_PERC,
				MR_RED_IN_BUFF_PERC,
				MR_RED_SLOWSTART_MAPS,
									 				
				MR_COMPRESS_MAP_OUT,
				MR_COMPRESS_OUT,
				"Use combiner"
		];
        setConfParamValues();
	    
	    confParamIntValues = [0, 1, 4, 5, 6];
	    confParamFloatValues = [2, 3, 7, 8, 9, 10];
	    confParamBoolValues = [11..13];
	    
	    updateConfParams = false;
	    
		// Initialize the data for the cluster specifications      	
        clusterKeys = [
   				"Number of nodes", 
   				"Map slots number per nodes", 
   				"Reduce slots number per nodes",
   				"Available memory per task (MB)",
		]; 
		setClusterValues();
				
		updateCluster = false;
		
	}
	
	// set clusterValues, used in initData() and import data from xml file, added by Fei Dong
	function setClusterValues() {
    	var tasktrackercollect : Collection = cluster.getAllTaskTrackersInfos();
     	var tasktrackerlist : ArrayList = new ArrayList();
     	tasktrackerlist.addAll(tasktrackercollect);
        clusterValues = [
   				cluster.getAllTaskTrackersInfos().size(),
   				(tasktrackerlist.get(0) as TaskTrackerInfo).getNumMapSlots(),
   				(tasktrackerlist.get(0) as TaskTrackerInfo).getNumReduceSlots(),
   				(ProfileUtils.getTaskMemory(conf) / (1024*1024)),
		];
	}
	
	// uesd for init the ConfParamValues initData and import data from xml file, added by Fei Dong
	function setConfParamValues() {
        confParamValues = [
			conf.getInt(MR_SORT_MB, DEF_SORT_MB),
			conf.getInt(MR_SORT_FACTOR, DEF_SORT_FACTOR),
			conf.getFloat(MR_SPILL_PERC, DEF_SPILL_PERC),
			conf.getFloat(MR_SORT_REC_PERC, DEF_SORT_REC_PERC),
			conf.getInt(MR_NUM_SPILLS_COMBINE, DEF_NUM_SPILLS_FOR_COMB),
			
			conf.getInt(MR_RED_TASKS, 1),
			conf.getInt(MR_INMEM_MERGE, DEF_INMEM_MERGE),
			conf.getFloat(MR_SHUFFLE_MERGE_PERC , DEF_SHUFFLE_MERGE_PERC),
			conf.getFloat(MR_SHUFFLE_IN_BUFF_PERC , DEF_SHUFFLE_IN_BUFF_PERC),
			conf.getFloat(MR_RED_IN_BUFF_PERC, DEF_RED_IN_BUFF_PERC),
			conf.getFloat(MR_RED_SLOWSTART_MAPS , DEF_RED_SLOWSTART_MAPS),
			
			conf.getBoolean(MR_COMPRESS_MAP_OUT, false),
			conf.getBoolean(MR_COMPRESS_OUT, false),
			(conf.get(MR_COMBINE_CLASS) != null
				and conf.getBoolean(STARFISH_USE_COMBINER, true))
	    ];
	}
	
	// Save the configuration parameter settings
	function saveConfParamSettings(conf: Configuration): Void {

		for (i in confParamIntValues) {
			conf.setInt(confParamKeys[i], confParamValues[i] as Integer);
		}
		
		for (i in confParamFloatValues) {
			conf.setFloat(confParamKeys[i], confParamValues[i] as Float);
		}
		
		conf.setBoolean(confParamKeys[confParamBoolValues[0]], confParamValues[confParamBoolValues[0]] as Boolean);
		conf.setBoolean(confParamKeys[confParamBoolValues[1]], confParamValues[confParamBoolValues[1]] as Boolean);
		conf.setBoolean(STARFISH_USE_COMBINER, confParamValues[confParamBoolValues[2]] as Boolean);
		
		ProfileUtils.setTaskMemory(conf, clusterValues[3] * 1024 * 1024);
	}
	
	
	// Save the cluster specifications settings
	function saveClusterSettings(): Void {
	    
        if (updateCluster) {
        	cluster = ClusterConfiguration.createClusterConfiguration(
        				cluster.getClusterName(), 1,
        				clusterValues[0], clusterValues[1], clusterValues[2],
						clusterValues[3] * 1024 * 1024);
        	ProfileUtils.setTaskMemory(conf, clusterValues[3] * 1024 * 1024);
			updateCluster = false;
        }
	}
	

	// Export a form's data as XML file
	function exportXMLFile(type: SettingsTableType, file: File): Void {
		exportDir = file.getParent();
		
    	if (type == SettingsTableType.CONF_PARAMS) {
    	    // Export the job configurations
    	    var exportConf = new Configuration(false);
			saveConfParamSettings(exportConf);
    		exportConf.writeXml(new PrintStream(file));
    		
		} else if (type == SettingsTableType.CLUSTER_SPECS) {
		    // Export the cluster specifications
		    saveClusterSettings();
		    XMLClusterParser.exportCluster(cluster, file);
		    
    	} else if (type == SettingsTableType.INPUT_SPECS) {
    	    // Export the input specifications
    	    XMLInputSpecsParser.exportMapInputSpecs(specs, file);
    	    
    	}
		Alert.inform("File saved at {file}");
	}

	// Import a form's data from XML file
	function importXMLFile(type: SettingsTableType, file: File): Void {
	    try {
	        importDir = file.getParent();
	        
	    	if (type == SettingsTableType.CONF_PARAMS) {
	    	    // import the job configurations
				conf.addResource(new Path(file.getAbsolutePath()));
				setConfParamValues();
			} else if (type == SettingsTableType.CLUSTER_SPECS) {
			    // Import the cluster specifications
			    cluster = XMLClusterParser.importCluster(file);
				setClusterValues();
	    	} else if (type == SettingsTableType.INPUT_SPECS) {
	    	    // Import the input specifications
	    	   	var newSpecs = XMLInputSpecsParser.importMapInputSpecs(file);
	    	   	specs.clear();
	    	   	specSize = 0;
	    	   	for(spec in newSpecs) {
	    	   		specs.add(spec);
	    	   	}
	    	   	specSize = specs.size();
	    	}
			Alert.inform("File imported successfully: {file}");
			
	    } catch(e: Exception) {
	        Alert.inform("Unable to import file: {file}");
	    }
	}
	
    ////////////////////////////
    // VIEW FUNCTIONS         //
    ////////////////////////////
    
    // Create the Configuration Parameters group
    function createConfParamGroup(): Void {
        
        // The border with the title
        var border = BorderGroup {
        	    groupX : bind startX
        	    groupY : bind startY
        	    groupWidth : bind groupsWidth
        	    groupHeight : bind groupsHeight * 0.42
        	    groupTitle : "Configuration Parameters"
        	    textWidth : 204.6
        	};
        
        // The actual table
        var table = ConfigTable{ 
                type: SettingsTableType.CONF_PARAMS
                readOnly: winType == WindowType.ANALYZE or winType == WindowType.OPTIMIZE 
            	configKeys: confParamKeys
            	configValues: bind confParamValues
            	
            	layoutX: bind border.groupX + 10
            	layoutY: bind border.groupY + 10
            	width: bind border.groupWidth - 20
            	height: bind border.groupHeight - 40
            };
        
        table.fixColumnWidth(0, 300);
        table.centerColumn(1);
        
        // The export button
       	var exportButton = ExportButton {
           layoutX : bind table.layoutX + border.groupWidth - 100
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.CONF_PARAMS
           defaultFile: "{job.getExecId()}_conf.xml"
       	}

        // The import button
       	var importButton = ImportButton {
           layoutX : bind table.layoutX + border.groupWidth - 190
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.CONF_PARAMS
		   visible: winType == WindowType.WHATIF
       	}; // end importButton
       	
        confParamForm = Group {
	        content : bind [ border, table, exportButton, importButton ]
        };
    }


	// Create the Cluster Configuration group
	function createClusterConfigGroup(): Void {
	    
        // The border with the title
        var border = BorderGroup {
    	    groupX : bind startX
    	    groupY : bind startY + confParamForm.boundsInLocal.height + groupsBuffer
    	    groupWidth : bind groupsWidth
    	    groupHeight : bind groupsHeight * 0.29
    	    groupTitle : "Cluster Specification"
    	    textWidth : 165.1
    	}

        // The actual table
        var table = ConfigTable{ 
            type: SettingsTableType.CLUSTER_SPECS
            readOnly: winType == WindowType.ANALYZE 
        	configKeys: clusterKeys
        	configValues: bind clusterValues
        	
        	layoutX: bind border.groupX + 10
        	layoutY: bind border.groupY + 10
        	width: bind border.groupWidth - 20
        	height: bind border.groupHeight - 40
        };
        
        table.fixColumnWidth(0, 300);
        table.centerColumn(1);
        
        // The export button
       	var exportButton = ExportButton {
           layoutX : bind table.layoutX + border.groupWidth - 100
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.CLUSTER_SPECS
           defaultFile: "{job.getExecId()}_cluster.xml"
       	}
       	
        // The import button
       	var importButton = ImportButton {
           layoutX : bind table.layoutX + border.groupWidth - 190
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.CLUSTER_SPECS
			visible: winType != WindowType.ANALYZE 
       	}; // end importButton
       	
        clusterForm = Group {
	        content : [ border, table, exportButton, importButton ]
        }
	}

	// Create the Input Specifications group
	function createInputSpecsGroup(): Void {
         // The border with the title
         var border = BorderGroup {
    	    groupX : bind startX
    	    groupY : bind startY + confParamForm.boundsInLocal.height + clusterForm.boundsInLocal.height + 2 * groupsBuffer
    	    groupWidth : bind groupsWidth
    	    groupHeight : bind groupsHeight * 0.29
    	    groupTitle : "Input Specification"
    	    textWidth : 147.6
    	};
    	
        // The actual table
        specSize = specs.size();
        var table = SpecTable{ 
            type: SettingsTableType.INPUT_SPECS
            readOnly: winType == WindowType.ANALYZE 
       		specs: bind specs
       		specSize: bind specSize
        	
        	layoutX: bind border.groupX + 10
        	layoutY: bind border.groupY + 10
        	width: bind border.groupWidth - 20
        	height: bind border.groupHeight - 40
        }; // end table
        
        // Add button
        var addButton = Button {
			text: "Add Row"
			font : COMMON_FONT_14
			width : 80
			height : 20
			layoutX : bind table.layoutX + border.groupWidth - 370
			layoutY : bind table.layoutY + table.height + 5
			visible : winType != WindowType.ANALYZE
			
			action: function() {
			    // Add a new row in the specs
				specs.add(new MapInputSpecs(0, 0, 0, false, DataLocality.DATA_LOCAL));
				specSize = specs.size();
				table.table.changeSelection(specSize - 1, 0, false, false);
			}
			
           onMouseEntered : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.HAND;
           }
           onMouseExited : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.DEFAULT;
           }
		}; // end addButton

		// Remove button
		var removeButton = Button {
			text: "Remove"
			font : COMMON_FONT_14
			width : 80
			height : 20
			layoutX : bind table.layoutX + border.groupWidth - 280
			layoutY : bind table.layoutY + table.height + 5
			visible : winType != WindowType.ANALYZE
			
			action: function() {
			    // Remove the current row from the specs
					if (table.selectionNum < 0 and table.specSize > 0) {
   	             		Alert.inform("Please choose one row");
   	             	} else if (table.specSize > 0) {
   	             	    var current = table.selectionNum;
            	    	specs.remove(current);
            	    	specSize = specs.size();
            	    	if (current < specSize) {
            	    		table.table.changeSelection(current, 0, false, false);
            	    	} else {
            	    		table.table.changeSelection(specSize - 1, 0, false, false);
            	    	}
             	    }
       		}
           		
           onMouseEntered : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.HAND;
           }
           onMouseExited : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.DEFAULT;
           }
        }; // end removeButton
        
        // The import button
       	var importButton = ImportButton {
           layoutX : bind table.layoutX + border.groupWidth - 190
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.INPUT_SPECS
		   visible: winType != WindowType.ANALYZE
       	}; // end importButton

        // The export button
       	var exportButton = ExportButton {
           layoutX : bind table.layoutX + border.groupWidth - 100
           layoutY : bind table.layoutY + table.height + 5
           type: SettingsTableType.INPUT_SPECS
           defaultFile: "{job.getExecId()}_input.xml"
       	}; // end exportButton
       	
        inputSpecsForm = Group {
	        content : [ border, table, addButton, removeButton, exportButton, importButton ]
       }
        
	}
	
	// Create a What-if or Optimize Button
	function createWhatifOptimizeButton(): Void {
       	whatIfButton = Button {
           text : 	if (winType == WindowType.WHATIF) {
           				"Ask What-if Question"
           			} else if (winType == WindowType.OPTIMIZE){
           			    "Optimize Job"
           			} else {
           				""
           			}
           font : COMMON_FONT_14
           layoutX : bind startX
           layoutY : bind height - 70
           
           action : function() : Void {
               
               	// Save current settings
			    if (updateConfParams) {
					saveConfParamSettings(conf);
					updateConfParams = false;
			    }
			    if (updateCluster) {
					saveClusterSettings();
					updateCluster = false;
			    }
			    
               if (winType == WindowType.WHATIF) {
                   (manager as VirtualMRJobManager).updateHadoopConfiguration(conf);
                   (manager as VirtualMRJobManager).updateClusterConfiguration(cluster);
                   (manager as VirtualMRJobManager).updateInputSpecifications(specs);
                   parentWindow.updateAllViews();
                   this.scene.cursor = Cursor.DEFAULT;
                   parentWindow.selectTimelineView();
                   parentWindow.selectTimelineButton();
               }
               if (winType == WindowType.OPTIMIZE) {
                   (manager as OptimizedJobManager).updateClusterConfiguration(cluster);
                   (manager as OptimizedJobManager).updateInputSpecifications(specs);
                   parentWindow.updateAllViews();
                   this.scene.cursor = Cursor.DEFAULT;
                   parentWindow.selectTimelineView();
                   parentWindow.selectTimelineButton();
               }
           }
           
           onMouseEntered : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.HAND;
           }
           onMouseExited : function(e : MouseEvent) : Void {
               this.scene.cursor = Cursor.DEFAULT;
           }
           visible: winType != WindowType.ANALYZE
       	}
	}
    
    /*
	 * create View, the general work includes collecting data, init the nodes, render the layout
	 * 	it is called when populateView() in Window.fx 
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
	    
	    // Try loading the profiles as well (needed to generate input specs correctly)
	    manager.loadProfilesForMRJob(job);
	    
	    // Get the job variables
        var mrJobId = job.getExecId();        
        conf = manager.getHadoopConfiguration(mrJobId);
        cluster = manager.getClusterConfiguration(mrJobId);
        specs = WhatIfUtils.generateMapInputSpecs(job);

        // Create the view
		initData();
		createConfParamGroup();
		createClusterConfigGroup();
		createInputSpecsGroup();
		createWhatifOptimizeButton();
    }
    
    /////////////////
    // create node //
    /////////////////
    override function create():Node {
        Group {
            content: bind [confParamForm, clusterForm, inputSpecsForm, whatIfButton, errorText]
       };        
    }
} // end of class

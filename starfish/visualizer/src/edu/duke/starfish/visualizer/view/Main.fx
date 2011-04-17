package edu.duke.starfish.visualizer.view;
import javafx.stage.Stage;
import javafx.scene.Group;
import javafx.scene.Scene;
import javafx.scene.text.Font;
import javafx.scene.text.Text; 
import javafx.scene.control.TextBox;
import javafx.scene.control.Label;
import javafx.scene.text.TextOrigin;
import javafx.scene.control.Button;
import javafx.scene.input.MouseEvent;
import javafx.stage.Stage;
import javafx.stage.Alert;
import javafx.scene.layout.VBox;
import javafx.scene.layout.HBox;
import javafx.scene.Cursor;
import javafx.scene.layout.LayoutInfo;
import javax.swing.JFileChooser;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import java.io.File;
import java.util.Random;
import edu.duke.starfish.visualizer.model.WindowType;
import edu.duke.starfish.visualizer.model.AppViewType;

import edu.duke.starfish.profile.profileinfo.execution.jobs.JobInfo;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profiler.MRJobLogsManager;
import edu.duke.starfish.profile.test.TestInfoManager;

import edu.duke.starfish.whatif.VirtualMRJobManager;
import edu.duke.starfish.jobopt.OptimizedJobManager;
import edu.duke.starfish.visualizer.view.Constants.*;

import edu.duke.starfish.visualizer.view.custom.Job;
import edu.duke.starfish.visualizer.view.custom.JobTable;

/**
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 * Main.fx
 *  
 * Entry for the visualizer,
 *  * Users can set the history path, profile path or data transfer path
 *  * Load jobs and create job table.
 *  * Generate a new window after clicking view button or what-if button 
 */
var jobTable : Group = Group {};
var chooseDirectory: Group = Group {};

var w = STAGE_WIDTH as Number on replace {validateSize()};
var h = STAGE_HEIGHT as Number on replace {validateSize()};

def buttonWidth = 160;

function validateSize() {
	if (w<650) w = 650;
	if (h<500) h = 500;    
}
// the main stage
var stage : Stage = Stage {
    title : "Starfish Visualizer"
    x : 200
    y : 100
    width : bind w with inverse
    height : bind h with inverse
    scene : Scene {
        content : bind [
        	bgImage,
        	chooseDirectory,
            jobTable
        ]
    }
    onClose : function() : Void {
        FX.exit();
    }
}

// Background Image
var bgImage = ImageView {
    fitWidth: bind stage.width
    fitHeight: bind stage.height
    image: getRandomImage()
    opacity: 0.6
}

function getRandomImage() {
    Image {
        backgroundLoading: true
        url: backgroundImage
    }
}

function createScene() {
    
 	var logManager : MRJobLogsManager;
    var logDir: String = FX.getProperty("javafx.user.dir");
    var historyDir: String;
    var dataTransferDir: String;
    var taskProfileDir: String;
    var jobProfileDir: String;
    var logDirNameInput : TextBox;
   
    var xBuffer = 30;
    
	jobTable.visible = false;
	chooseDirectory.content = VBox {
        layoutX: xBuffer
        layoutY: 20
        spacing: 10
	    content: [
	    Text {
            content : "Select the input directory"
            font : Font {size : 20}
            textOrigin : TextOrigin.TOP
	     }
	    // log directory
		HBox {
	    spacing: 20 
	    content: [
			Label {
			    translateY: 3
		 	    font : Font { size : 16 }
		   	    text: "Log Directory:"
			}
			logDirNameInput = TextBox {
			    text: "Choose a dir"
			    font: Font {size: 16}
			    columns: 35
			    selectOnFocus: true
			}
            Button {
	            text: "Browse..."
	            font: Font {size: 16}
	            action: function() {
		             var fc = new JFileChooser();
		             fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		             fc.setCurrentDirectory(new File(logDir));
		             var retVal = fc.showOpenDialog(null);
		             if(retVal == JFileChooser.APPROVE_OPTION) {
		                 logDirNameInput.text = fc.getSelectedFile().getAbsolutePath();
		             }
	           	}
	           	
	           onMouseEntered : function(e : MouseEvent) : Void {
	               stage.scene.cursor = Cursor.HAND;
	           }
	           onMouseExited : function(e : MouseEvent) : Void {
	               stage.scene.cursor = Cursor.DEFAULT;
	           }
	        }
	     ]
     	},
     	
     	Button {
            text : "Submit"
            styleClass: "button"
            font: Font {size: 16}
            layoutInfo: LayoutInfo {
            	width: buttonWidth
            }
            
       	    action : function() : Void {
       	        
       	        if (logDirNameInput.text == "Choose a dir" or logDirNameInput.text == "") {
       	            Alert.inform("Please specify a log directory");
       	            return;
       	        }
       	        
       	        // Get the log directory
				logDir = logDirNameInput.text;
     	        historyDir = "{logDir}/history"; 
     	        taskProfileDir = "{logDir}/task_profiles";
     	        jobProfileDir = "{logDir}/job_profiles";
     	        dataTransferDir = "{logDir}/transfers";
     	        
     	        logManager = new MRJobLogsManager();
     	        
     	        if (new File(historyDir).exists()) {
     				logManager.setHistoryDir(historyDir);
     	        } else {
     	            Alert.inform("Can not find the history directory");
     	            return;
     	        }
     	        
     	        if (new File(jobProfileDir).exists())
	     	        logManager.setJobProfilesDir(jobProfileDir);
     	        if (new File(taskProfileDir).exists())
	     	        logManager.setTaskProfilesDir(taskProfileDir);
	     	    if (new File(dataTransferDir).exists())
     	         	logManager.setTransfersDir(dataTransferDir);
     	        
     	        jobs = for(j in logManager.getAllMRJobInfos()) 
     	        	Job {
     	              job : j
     	              name : j.getName()
     	              execId : j.getExecId()
     	              user : j.getUser()
     	              startTime : j.getStartTime().toString()
     	              endTime : j.getEndTime().toString()
     	              status : j.getStatus().toString()
     	        	}
     	        if (sizeof jobs > 0)
     	        	jt.table.setRowSelectionInterval(0,0);
				jobTable.visible  = true;
     	    }
            onMouseEntered : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.HAND;
            }
            onMouseExited : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.DEFAULT;
            }
     	}
	  ]
	}
     
	var jobs: Job[];
    
    var title : Text = Text {
        content : "Select a job"
        font : Font {size : 20}
        x : xBuffer
        y : bind chooseDirectory.boundsInLocal.height + 40;
        textOrigin : TextOrigin.TOP
    }
        
    var jt = JobTable {
        jobs : bind jobs
        layoutX : xBuffer
        layoutY : bind chooseDirectory.boundsInLocal.height + title.boundsInLocal.height + 40
        width : bind stage.width - 2*xBuffer as Integer
        height : bind stage.height - (chooseDirectory.boundsInLocal.height + title.boundsInLocal.height + 40) - 80 as Integer
    }
    	
    jobTable.content = [
        title,
        jt,
        Button {
            text : "Analyze Job"
            styleClass: "button"
            font: Font {size: 16}
            layoutX : xBuffer
            layoutY : bind stage.height - 70
            width: buttonWidth
            action : function() : Void {
                var job : MRJobInfo = if(jt.select >= 0) (jobs[jt.select].job as MRJobInfo) else (jobs[0].job as MRJobInfo);
                var window = createWindow(job, logManager, WindowType.ANALYZE, AppViewType.TIMELINE);
               	window.title = "Analyze Job: {job.getExecId()} ({job.getName()})";
            }
            onMouseEntered : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.HAND;
            }
            onMouseExited : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.DEFAULT;
            }
        }
        Button {
            text : "What-if Question"
            styleClass: "button"
            font: Font {size: 16}
            layoutX : 2*xBuffer + buttonWidth
            layoutY : bind stage.height - 70
            width: buttonWidth
            action : function() : Void {
                var job : MRJobInfo = if(jt.select >= 0) (jobs[jt.select].job as MRJobInfo) else (jobs[0].job as MRJobInfo);
                
				if (jobProfileDir == "" and taskProfileDir == "") {
					Alert.inform("Please specify either Job Profiles or Task Profiles directory");
     	        	return ;
				}
                
                if (not logManager.loadProfilesForMRJob(job)) {
					Alert.inform("Unable to load the job profile!");
					return ;                    
                }
                
                var conf = logManager.getHadoopConfiguration(job.getExecId());
                var cluster = logManager.getClusterConfiguration(job.getExecId());
                var manager = new VirtualMRJobManager(job, conf, cluster);
                job = manager.getMRJobInfo(job.getExecId());
                
                var window = createWindow(job, manager, WindowType.WHATIF, AppViewType.SETTINGS);
               	window.title = "What-if Question: {job.getExecId()} ({job.getName()})";
            }
            onMouseEntered : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.HAND;
            }
            onMouseExited : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.DEFAULT;
            }
        }
        Button {
            text : "Optimize Job"
            styleClass: "button"
            font: Font {size: 16}
            layoutX : 3*xBuffer + 2*buttonWidth
            layoutY : bind stage.height - 70
            width: buttonWidth
            action : function() : Void {
                var job : MRJobInfo = if(jt.select >= 0) (jobs[jt.select].job as MRJobInfo) else (jobs[0].job as MRJobInfo);
                
				if (jobProfileDir == "" and taskProfileDir == "") {
					Alert.inform("Please specify either Job Profiles or Task Profiles directory");
     	        	return ;
				}
                
                if (not logManager.loadProfilesForMRJob(job)) {
					Alert.inform("Unable to load the job profile!");
					return ;                    
                }
                
                var conf = logManager.getHadoopConfiguration(job.getExecId());
                var cluster = logManager.getClusterConfiguration(job.getExecId());
                var manager = new OptimizedJobManager(job, conf, cluster);
                job = manager.getMRJobInfo(job.getExecId());
                
                var window = createWindow(job, manager, WindowType.OPTIMIZE, AppViewType.SETTINGS);
               	window.title = "Optimize Job: {job.getExecId()} ({job.getName()})";
            }
            onMouseEntered : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.HAND;
            }
            onMouseExited : function(e : MouseEvent) : Void {
                stage.scene.cursor = Cursor.DEFAULT;
            }
        }
    ];
}

/*
 * create a new window for views, it is called when clicking button
 */
function createWindow(job : MRJobInfo, logManager : IMRInfoManager, 
					  winType: WindowType, defaultView: AppViewType): Window {
    var window  = Window{
		width : STAGE_WIDTH
		height : STAGE_HEIGHT
		job: job
		manager: logManager
	    winType: winType;
		defaultView: defaultView;
	};
	window.selectView(defaultView);
    return window;
}

/*
 * The main fucntion
 */
function run() : Void {
    createScene();
}

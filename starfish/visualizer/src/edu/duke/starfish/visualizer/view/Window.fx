package edu.duke.starfish.visualizer.view;
import javafx.stage.Stage;
import javafx.scene.Scene;
import javafx.scene.text.Text;
import javafx.scene.paint.Color;
import javafx.scene.layout.HBox;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.ToggleGroup;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.LayoutInfo;
import javafx.scene.shape.Rectangle;
import javafx.scene.Cursor;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.visualizer.model.WindowType;
import edu.duke.starfish.visualizer.model.AppViewType;
import java.util.HashMap;
import javafx.util.Math;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import edu.duke.starfish.visualizer.view.Constants.*;

/*
 *
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 * Window.fx
 *
 * Create a new window with toggle buttons
 *
 */
public class Window extends Stage{
    override public var title;
	var buttonWidth = bind (width - 10) / 5.0;	
	var buttonHeight = bind Math.max(30, Math.min(40, height / 20));
	override var width = STAGE_WIDTH as Number on replace {validateSize()};
	override var height = STAGE_HEIGHT as Number on replace {validateSize()};
	
	function validateSize() {
		if (width < 650) width = 650;
		if (height < 500) height = 500;    
	}	
	public var manager : IMRInfoManager;
	public var job: MRJobInfo;
	public var winType: WindowType;
	public var defaultView: AppViewType;

	var currentView: AppView;
	var viewMap = new HashMap();
	var updateMap = new HashMap();
	
	// Background Image
	var bgImage = ImageView {
	    fitWidth: bind width
	    fitHeight: bind height
	    image: Image {
	            backgroundLoading: true
	            url: backgroundImage
	        }
	    opacity: 0.5
	}
	
	public var buttonGroup: ToggleGroup = new ToggleGroup;
	
    ////////////////////////////////////////////////////////////
    // Below list 5 buttons which are managed by buttonGroup ///
    //				each button bind with a view             ///
    ////////////////////////////////////////////////////////////
    // Timeline
	var timelineButton: ToggleButton = ToggleButton {
	    id: AppViewType.TIMELINE.toString()
	    text: "Timeline"
	    font : COMMON_FONT_16
	    selected: defaultView == AppViewType.TIMELINE
	    toggleGroup: buttonGroup
	    layoutInfo: LayoutInfo {
	        width: bind buttonWidth
	        height: bind buttonHeight
	    }
	    onMouseClicked:function (e:MouseEvent) {
	        selectTimelineView();
	    }
        onMouseEntered : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.HAND;
        }
        onMouseExited : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.DEFAULT;
        }
	}
	
    // Data Skew
    var dataSkewButton: ToggleButton = ToggleButton {
        id: AppViewType.DATASKEW.toString()
        text: "Data Skew"
        font : COMMON_FONT_16
	    selected: defaultView == AppViewType.DATASKEW
        toggleGroup: buttonGroup
        layoutInfo: LayoutInfo {
            width: bind buttonWidth
            height: bind buttonHeight
        }
	    onMouseClicked:function (e:MouseEvent) {
	        selectDataSkewView();
	    }
        onMouseEntered : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.HAND;
        }
        onMouseExited : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.DEFAULT;
        }
    }
	
	// Data Flow
    var dataFlowButton: ToggleButton = ToggleButton {
        id: AppViewType.DATAFLOW.toString()
        text: "Data Flow"
        font : COMMON_FONT_16
	    selected: defaultView == AppViewType.DATAFLOW
        toggleGroup: buttonGroup
        layoutInfo: LayoutInfo {
            width: bind buttonWidth
            height: bind buttonHeight
        }
	    onMouseClicked:function (e:MouseEvent) {
	        selectDataFlowView();
	    }
        onKeyPressed: function (ke: KeyEvent) {
        }
        onMouseEntered : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.HAND;
        }
        onMouseExited : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.DEFAULT;
        }
    }
    
    // Profiles
    var profilesButton: ToggleButton = ToggleButton {
        id: AppViewType.PROFILES.toString()
        text: "Profiles"
        font : COMMON_FONT_16
	    selected: defaultView == AppViewType.PROFILES
        toggleGroup: buttonGroup
        layoutInfo: LayoutInfo {
            width: bind buttonWidth
            height: bind buttonHeight
        }
	    onMouseClicked:function (e:MouseEvent) {
	        selectProfilesView();
	    }
        onMouseEntered : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.HAND;
        }
        onMouseExited : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.DEFAULT;
        }
    }
    
    // Settings
    var settingsButton: ToggleButton = ToggleButton {
        id: AppViewType.SETTINGS.toString();
        text: "Settings"
        font : COMMON_FONT_16
        toggleGroup: buttonGroup
        selected: defaultView == AppViewType.SETTINGS
        layoutInfo: LayoutInfo {
            width: bind buttonWidth
            height: bind buttonHeight
        }
	    onMouseClicked:function (e:MouseEvent) {
	        selectSettingsView();
	    }
        onMouseEntered : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.HAND;
        }
        onMouseExited : function(e : MouseEvent) : Void {
            scene.cursor = Cursor.DEFAULT;
        }
    }
    
    // a group of top buttons
    var topButtonBox: HBox = HBox {
        layoutX : 1
        layoutY : 1
        content: [timelineButton, dataSkewButton, dataFlowButton, profilesButton, settingsButton]
        spacing: 0
	}
	
	/**
	 * Select the provided view
	**/
	public function selectView(type: AppViewType) {
	    if (type == AppViewType.TIMELINE){
	        selectTimelineView();
			timelineButton.selected = true;
			timelineButton.requestFocus();
	    }
	    if (type == AppViewType.DATASKEW){
	        selectDataSkewView();
			dataSkewButton.selected = true;
			dataSkewButton.requestFocus();
	    }
	    if (type == AppViewType.DATAFLOW){
	        selectDataFlowView();
			dataFlowButton.selected = true;
			dataFlowButton.requestFocus();   
	    }
	    if (type == AppViewType.PROFILES){
	        selectProfilesView();
			profilesButton.selected = true;
			profilesButton.requestFocus();
	    }
	    if (type == AppViewType.SETTINGS){
	        selectSettingsView();
			settingsButton.selected = true;
			settingsButton.requestFocus();
	    }
	}
	
	/**
	 * Select Timeline View
	**/
	public function selectTimelineView() {
		if (not viewMap.containsKey(AppViewType.TIMELINE)) {
		    var view = TimelineView{
		    		width: bind width, 
		    		height: bind height
		   			job: job
		   			manager: manager
		   			winType: winType
		   			};
		   	view.createView();
	      	viewMap.put(AppViewType.TIMELINE, view);
	      	updateMap.put(AppViewType.TIMELINE, false);
		}
		updateCurrentView(AppViewType.TIMELINE);
	}

	/**
	 * Select DataSkew View
	**/
	public function selectDataSkewView() {
	    if (not viewMap.containsKey(AppViewType.DATASKEW)) {
	        var view = DataSkewView {
	    	   		width: bind width, 
	          		height: bind height
	        		job: job
	        		manager: manager
	        		winType: winType
	        	};
	        view.createView();
   	        viewMap.put(AppViewType.DATASKEW, view);
   	        updateMap.put(AppViewType.DATASKEW, false);
	    }
		updateCurrentView(AppViewType.DATASKEW);
	}

	/**
	 * Select DataFlow View
	**/
	public function selectDataFlowView() {
        if (not viewMap.containsKey(AppViewType.DATAFLOW)) {
            var view = DataFlowView {
            		width: bind width, 
            		height: bind height
           			job: job
           			manager: manager
           			winType: winType
       			};
           	view.createView();
           	viewMap.put(AppViewType.DATAFLOW, view);
           	updateMap.put(AppViewType.DATAFLOW, false);
        }
		updateCurrentView(AppViewType.DATAFLOW);
	}
	
	/**
	 * Select Profiles View
	**/
	public function selectProfilesView() {
        if (not viewMap.containsKey(AppViewType.PROFILES)) {
            var view = ProfileView{
            		width: bind width, 
            		height: bind height
           			job: job
           			manager: manager
           			winType: winType
   			};
           	view.createView();
           	viewMap.put(AppViewType.PROFILES, view);
           	updateMap.put(AppViewType.PROFILES, false);
        }
		updateCurrentView(AppViewType.PROFILES);
	}

	/**
	 * Select Settings View
	**/
	public function selectSettingsView() {
        if (not viewMap.containsKey(AppViewType.SETTINGS)) {
            var view = SettingsView{
	           		width: bind width, 
	           		height: bind height
	       			job: job
	       			manager: manager
	       			winType: winType
	       			parentWindow : this
			};
   			view.createView();
   	       	viewMap.put(AppViewType.SETTINGS, view);
   	       	updateMap.put(AppViewType.SETTINGS, false);
	    }
		updateCurrentView(AppViewType.SETTINGS);
	}
	

	/**
	 * Make the button selected. NOTE: Assumes select*View() was called before
	 * Warning: Cannot combine these functions with the select*View due to
	 * possible cyclic dependencies
 	**/
	public function selectTimelineButton() {
		timelineButton.selected = true;
		timelineButton.requestFocus();
    }
    public function selectDataSkewButton() {
		dataSkewButton.selected = true;
		dataSkewButton.requestFocus();
    }
    public function selectDataFlowButton() {
		dataFlowButton.selected = true;
		dataFlowButton.requestFocus();   
    }
    public function selectProfilesButton() {
		profilesButton.selected = true;
		profilesButton.requestFocus();
    }
    public function selectSettingsButton() {
		settingsButton.selected = true;
		settingsButton.requestFocus();
    }
	
	/**
	 * Updates the current view. Will recreate the current view if the update flag is set
	**/
	function updateCurrentView(type: AppViewType) {
	    
	    // Select the input view as the new current view
		currentView = viewMap.get(type) as AppView;
		
		if (updateMap.get(type) as Boolean) {
		    // Recreate the current view
		   	currentView.createView();
		   	updateMap.put(type, false);
		}
	}
	
	/**
	 * Set the flags to update all views
	**/
	public function updateAllViews() {
	    for (key in updateMap.keySet()) {
	        updateMap.put(key, true);
	    }
	}
	
    /////////////////
    // create scene //
	/////////////////	
    override public var scene = Scene{
        content: bind [
			topButtonBox, currentView
        ]
    }
}

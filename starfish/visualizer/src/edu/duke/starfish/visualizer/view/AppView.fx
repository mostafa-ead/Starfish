package edu.duke.starfish.visualizer.view;
import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import edu.duke.starfish.profile.profileinfo.IMRInfoManager;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.visualizer.model.WindowType;


/**
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 * AppView.fx
 * 
 * Base Class, extract some common variables and functions. 
 * DataSkewView, DataTransferView, ProfileView, SpecificationView, TimelineView inherit from this class 
 *  and should implement createView() method
 */public abstract class AppView extends CustomNode {
    protected var view: Node;
    var viewContent: Node[] = [];
	public var width: Number;
	public var height: Number;
     
	public var manager : IMRInfoManager;
   	public var job: MRJobInfo;
   	public var winType: WindowType;
     
    protected function deleteData() {
    } 
     
    // inherit class should implement this function
    protected abstract function createView(): Void;
     
    public override function create(): Node {
		createView();
		insert view into viewContent;
		return Group {
			content: bind viewContent
		}
 	}
 }
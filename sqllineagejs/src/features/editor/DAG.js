import React, { useEffect, useRef } from "react";
import CytoscapeComponent from "react-cytoscapejs";
import dagre from "cytoscape-dagre";
import cytoscape from "cytoscape";
import { useDispatch, useSelector } from "react-redux";
import { selectEditor, setDagLevel } from "./editorSlice";
import { Loading } from "../widget/Loading";
import { LoadError } from "../widget/LoadError";
import {
  SpeedDial,
  SpeedDialIcon,
  ToggleButton,
  ToggleButtonGroup,
} from "@material-ui/lab";
import { makeStyles } from "@material-ui/core/styles";
import SpeedDialAction from "@material-ui/lab/SpeedDialAction";
import SaveAltIcon from "@material-ui/icons/SaveAlt";
import ZoomInIcon from "@material-ui/icons/ZoomIn";
import SettingsBackupRestoreIcon from "@material-ui/icons/SettingsBackupRestore";
import ZoomOutIcon from "@material-ui/icons/ZoomOut";
import TableChartIcon from "@material-ui/icons/TableChart";
import ViewWeekIcon from "@material-ui/icons/ViewWeek";
import { Tooltip } from "@material-ui/core";

cytoscape.use(dagre);

const useStyles = makeStyles((theme) => ({
  speedDial: {
    position: "absolute",
    bottom: theme.spacing(5),
    right: theme.spacing(2),
  },
  floatingButton: {
    position: "absolute",
    right: theme.spacing(0),
    bottom: theme.spacing(50),
    zIndex: 100,
  },
}));

export function DAG(props) {
  const classes = useStyles();
  const dispatch = useDispatch();
  const editorState = useSelector(selectEditor);
  const [open, setOpen] = React.useState(false);
  const cyRef = useRef(null);
  console.log("editorState");
  console.log(editorState);
  const layoutTable = {
    name: "dagre",
    rankDir: "TB",
    rankSep: 200,
    nodeSep: 50,
  };
  const layoutColumn = {
    name: "dagre",
    rankDir: "LR",
    rankSep: 60,
    nodeSep: 15,
  };

  const handleSave = () => {
    if (cyRef.current) {
      let cy = cyRef.current._cy;
      let aDownloadLink = document.createElement("a");
      aDownloadLink.download = `${editorState.file}.jpg`;
      aDownloadLink.href = cy.jpg({ full: true, quality: 1 });
      aDownloadLink.click();
      setOpen(false);
    }
  };

  const handleLevel = (event, value) => {
    if (cyRef.current) {
      let cy = cyRef.current._cy;
      let data =
        value === "column" ? editorState.dagColumn : editorState.dagContent;
      ////console.log("cy : ", cy);
      let layout = value === "column" ? layoutColumn : layoutTable;
      cy.elements().remove();
      cy.add(data);
      cy.layout(layout).run();
      dispatch(setDagLevel(value));
    }
  };

  useEffect(() => {
    if (cyRef.current) {
      let cy = cyRef.current._cy;
      cy.on("mouseover", "node", function (e) {
        let sel = e.target;
        // current node has parent node: children node to highlight in column lineage
        // or no parent node in the graph, meaning table lineage: every node is children node
        if (
          sel.parent().size() > 0 ||
          cy
            .elements()
            .filter((node) => node.isParent())
            .size() === 0
        ) {
          let elements = sel.union(sel.successors()).union(sel.predecessors());
          elements.addClass("highlight");
          cy.elements()
            .filter((node) => node.isChild())
            .difference(elements)
            .addClass("semitransparent");
        }
        // if current node has parent candidates in column lineage mode, add them temporarily
        let parents = sel.data().parent_candidates;
        if (parents !== undefined && parents.length > 1) {
          let tmp_elem = parents
            .map((p) => [
              { data: { id: p.name, type: p.type, temporary: true } },
              {
                data: {
                  id: sel.data().id + "-" + p.name,
                  source: sel.data().id,
                  target: p.name,
                  temporary: true,
                },
              },
            ])
            .reduce((x, y) => x.concat(y));
          if (
            cy
              .elements()
              .filter((n) => n.data().temporary === true)
              .size() === 0
          ) {
            let zoom = cy.zoom();
            let pan = cy.pan();
            cy.add(tmp_elem);
            cy.elements()
              .filter((n) => n.data().temporary === undefined)
              .lock();
            cy.elements()
              .filter(
                (n) =>
                  n.data().temporary === true || n.data().id === sel.data().id
              )
              .layout({
                boundingBox: {
                  x1: sel.position().x,
                  y1: sel.position().y,
                  w: 200,
                  h: 200,
                },
                circle: true,
                name: "breadthfirst",
              })
              .run();
            cy.viewport({
              zoom: zoom,
              pan: pan,
            });
          }
        }
      });
      cy.on("mouseout", "node", function () {
        cy.elements().removeClass("semitransparent");
        cy.elements().removeClass("highlight");
        cy.remove(cy.elements().filter((n) => n.data().temporary === true));
        cy.elements()
          .filter((n) => n.data().temporary === undefined)
          .unlock();
      });
    }
  });

  if (editorState.dagStatus === "loading") {
    return <Loading minHeight={props.height} />;
  } else if (editorState.dagStatus === "failed") {
    return (
      <LoadError
        minHeight={props.height}
        message={
          editorState.dagError +
          "\nPlease check your SQL code for potential syntax error in Script View."
        }
      />
    );
  } else if (editorState.dagContent.length === 0) {
    let message,
      info = false;
    if (editorState.editable) {
      if (editorState.contentComposed === "") {
        message =
          "Welcome to SQLLineage Playground.\n" +
          "Just paste your SQL in Script View and switch back here, you'll get DAG visualization for your SQL code.\n" +
          "Or select SQL file on the left directory tree for visualization.\n" +
          "Have fun!";
        info = true;
      } else {
        message =
          "No Lineage Info found in your SQL.\nPlease review your code in Script View.";
      }
    } else {
      message = `No Lineage Info found in SQL file ${editorState.file}.\nPlease review the code in Script View.`;
    }
    return <LoadError minHeight={props.height} message={message} info={info} />;
  } else {
    const stylesheet = [
      {
        selector: 'node[type = "Table"], node[type = "SubQuery"]',
        style: {
          height: 10,
          width: 10,
          content: (elem) => elem.data()["id"],
          "text-valign": "top",
          "text-halign": "right",
          "font-size": 10,
          color: "#35393e",
          "background-color": "#2fc1d3",
          "border-color": "#000",
          "border-width": 1,
          "border-opacity": 0.8,
        },
      },
      {
        selector: 'node[type = "Column"]',
        style: {
          height: 10,
          width: 10,
          "text-wrap": "wrap",
          "text-max-width": 200,
          content: (elem) => elem.data()["formula"],
          "text-valign": "top",
          "text-halign": "center",
          "font-size": 10,
          "font-weight": "bold",
          color: "red", //"#35393e",
          "background-color": "#2fc1d3",
          "border-color": "#000",
          "border-width": 1,
          "border-opacity": 0.8,
        },
      },
      {
        selector: 'node[type = "Where"], node[type = "Groupby"]',
        style: {
          content: (elem) => elem.data()["val"],
          shape: "rectangle",
          height: (elem) => Math.ceil(elem.data()["len"] / 200) * 80,
          width: 200,
          "text-wrap": "wrap",
          "text-max-width": 200,
          "text-justification": "center",
          "background-opacity": 0.1,
          "text-halign": "center",
          "text-valign": "center",
          "font-size": 10,
          color: "#35393e",
          "background-color": "#f5f5f5",
          "border-color": "black",
          "border-width": 1,
          "border-opacity": 0.8,
        },
      },
      {
        selector: ":parent",
        style: {
          content: (elem) => elem.data()["type"] + ": " + elem.data()["id"],
          "font-size": 16,
          "font-weight": "bold",
          "text-halign": "center",
          "text-valign": "top",
        },
      },
      {
        selector: ':parent[type = "Table"], :parent[type = "Path"]',
        style: {
          "background-color": "#f5f5f5",
          "border-color": "#00516c",
        },
      },
      {
        selector: ':parent[type = "SubQuery"]',
        style: {
          "background-color": "#f5f5f5",
          "border-color": "#b46c4f",
        },
      },
      {
        selector: ':parent[type = "Table or SubQuery"]',
        style: {
          "background-color": "#f5f5f5",
          "border-color": "#b46c4f",
          "border-style": "dashed",
        },
      },
      {
        selector: "edge",
        style: {
          width: 1,
          "line-color": "#9ab5c7",
          "target-arrow-color": "#9ab5c7",
          "target-arrow-shape": "triangle",
          "arrow-scale": 0.8,
          "curve-style": "taxi", //"bezier",
        },
      },
      {
        selector: ".highlight",
        style: {
          "background-color": "#076fa1",
        },
      },
      {
        selector: ".semitransparent",
        style: { opacity: "0.2" },
      },
      {
        selector: "node[temporary]",
        style: {
          "background-color": "#f5f5f5",
          "border-color": "#b46c4f",
          "border-style": "dashed",
          content: (elem) => elem.data()["type"] + ": " + elem.data()["id"],
          "font-size": 16,
          "font-weight": "bold",
          shape: "rectangle",
          "text-halign": "center",
          "text-valign": "top",
        },
      },
      {
        selector: "edge[temporary]",
        style: {
          "line-color": "#b46c4f",
          "line-style": "dashed",
          "target-arrow-color": "#b46c4f",
        },
      },
    ];
    const style = { width: props.width, height: props.height };
    return (
      <div>
        <CytoscapeComponent
          elements={editorState.dagContent}
          stylesheet={stylesheet}
          style={style}
          layout={layoutTable}
          zoom={1}
          minZoom={0.5}
          maxZoom={2}
          wheelSensitivity={0.2}
          ref={cyRef}
        />
        <ToggleButtonGroup
          orientation="vertical"
          value={editorState.dagLevel}
          exclusive
          onChange={handleLevel}
          aria-label="lineage level"
          className={classes.floatingButton}
        >
          <ToggleButton value="table">
            <Tooltip title="Table Level Lineage">
              <TableChartIcon />
            </Tooltip>
          </ToggleButton>
          <ToggleButton value="column">
            <Tooltip title="Column Level Lineage">
              <ViewWeekIcon />
            </Tooltip>
          </ToggleButton>
        </ToggleButtonGroup>
        <SpeedDial
          ariaLabel="SpeedDial example"
          className={classes.speedDial}
          hidden={false}
          icon={<SpeedDialIcon />}
          onClose={() => setOpen(false)}
          onOpen={() => setOpen(true)}
          open={open}
          direction="left"
          FabProps={{ size: "small" }}
        >
          <SpeedDialAction
            title="Save"
            icon={<SaveAltIcon />}
            onClick={handleSave}
          />
          <SpeedDialAction
            title="Zoom In"
            icon={<ZoomInIcon />}
            onClick={() => {
              cyRef.current._cy.zoom(cyRef.current._cy.zoom() + 0.1);
            }}
          />
          <SpeedDialAction
            title="Auto Fit"
            icon={<SettingsBackupRestoreIcon />}
            onClick={() => {
              cyRef.current._cy.fit();
            }}
          />
          <SpeedDialAction
            title="Zoom Out"
            icon={<ZoomOutIcon />}
            onClick={() => {
              cyRef.current._cy.zoom(cyRef.current._cy.zoom() - 0.1);
            }}
          />
        </SpeedDial>
      </div>
    );
  }
}

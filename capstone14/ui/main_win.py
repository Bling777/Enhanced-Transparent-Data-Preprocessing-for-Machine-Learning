
import sys  
import os
import pandas as pd
from PyQt5.QtWidgets import QComboBox, QDialog
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import networkx as nx

from pandas import DataFrame, read_csv

from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.ui.add_process_step import AddProcessStepWin
from capstone14.ui.data_trans_type import DataTransType, run_data_transformation
from capstone14.db.db_functions import create_run


class MainUIWindow(QWidget):
    def __init__(self):
        super(MainUIWindow, self).__init__()        
        self.initUI()

        self.run = None
        self.dag = nx.DiGraph()
        self.add_pstep = AddProcessStepWin()

    def initUI(self):
        self.setWindowTitle('Transparent Data Preprocessing System')
        buttons = (('Add Raw Data', self.add_raw_data),
                   ('Add Step', self.add_pstep), 
                   ('Run Pipeline', self.run_pipeline), 
                   ('Show Profile', self.show_profile), 
                   ('Compare Profiles', self.compare_profile), 
                   ('Save Pipeline', self.save_profile), 
                   ('Load Pipeline', self.load_profile))

        grid = QGridLayout()
        self.setLayout(grid)

        # buttons for main functions
        layout = QHBoxLayout()
        for btn in buttons:
            button = QPushButton(btn[0], self)
            button.clicked.connect(btn[1])
            layout.addWidget(button)
            layout.setSpacing(10)
        hGroupBox = QGroupBox()
        hGroupBox.setLayout(layout)
        grid.addWidget(hGroupBox, 0, 0)

        # Directed Acyclic Graph
        figure = plt.figure()
        self.canvas = FigureCanvas(figure)        
        grid.addWidget(self.canvas, 1, 0, 9, 9)          

        # set windows size and position (center)
        self.setGeometry(100, 100, 800, 600)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def draw_DAG(self):
        # print(self.dag.nodes.data())
        # print(self.dag.edges.data())

        # set each position of nodes
        for layer, nodes in enumerate(nx.topological_generations(self.dag)):
            for node in nodes:
                self.dag.nodes[node]["layer"] = layer
        pos = nx.multipartite_layout(self.dag, subset_key="layer")

        plt.clf()
        nx.draw(self.dag, pos=pos, with_labels=True, node_shape='s',
                node_color='lightblue', node_size=1000, font_size=10, font_weight='bold')
        self.canvas.draw_idle()

    def add_raw_data(self):
        # Add raw data file into raw_data list
        file_path, _ = QFileDialog.getOpenFileName(self, 'Open Raw File', '', 'CSV Files (*.csv)')
        
        if file_path:  # If a file is selected
            # check if the file is already included
            if len([x for x,y in self.dag.nodes(data=True) if y['type']=='raw' and y['path']==file_path]) > 0:
                QMessageBox.warning(self, 'Warning', 'The file is already included')
                return
            else:
                id = len(self.dag.nodes)  # Assign a unique ID
                name = f'R{id}. {os.path.basename(file_path)}'
                file_desc = f'Raw data file {len(self.dag.nodes) + 1}'  # Simple description

                self.dag.add_node(name, id=id, type='raw', path=file_path, description=file_desc)
                self.draw_DAG()  # Update DAG display
    
    def add_pstep(self):
        AddProcessStepWin.list_input_nodes(list(self.dag.nodes))

        if AddProcessStepWin.selected_pstep != None:
            id = len(self.dag.nodes)  # Assign a unique ID
            step_name = f'S{id}. {AddProcessStepWin.selected_pstep.value}'
            self.dag.add_node(step_name, id=id, type='step', trans_type=AddProcessStepWin.selected_pstep)

            for input_nd in AddProcessStepWin.selected_input_nodes:
                self.dag.add_edge(input_nd, step_name)

            self.draw_DAG()  # Update DAG display

    def run_pipeline(self):
        if self.dag.number_of_nodes() == 0:
            QMessageBox.warning(self, "Warning", "No data or steps in the preprocessing")
            return

        print("##### Start: run_pipeline #####")

        self.run = None
        self.run = PipelineRun()

        # iterate according the postion of nodes
        for node_generation in [sorted(generation) for generation in nx.topological_generations(self.dag)]:
            for node_name in node_generation:
                print(f"### Read {node_name} ###")
                node = self.dag.nodes[node_name]
                if node['type'] == 'raw': # if the node is a raw data
                    df = read_csv(node['path'])
                    node['dataset_id'] = self.run.add_dataset(df)

                elif node['type'] == 'step': # if the node is a data transformation step
                    input_dataset_ids = []
                    # iterate the input nodes of the node
                    for input_node_name in sorted(nx.ancestors(self.dag, node_name)):
                        input_node = self.dag.nodes[input_node_name]
                        
                        # check if the input node is an adjacent node (includes only immediately proceeding nodes)
                        if node_name in list(self.dag.adj[input_node_name]):
                            input_dataset_ids.append(input_node['dataset_id'])
                    node['dataset_id'] = run_data_transformation(self.run, node['trans_type'], input_dataset_ids)

                print(f"### Done {node_name} ###")

    def show_profile(self):
        if self.run is None:
            QMessageBox.warning(self, "Warning", "No pipeline has been run yet.")
            return

        # Open the dialog to let user select a node for profiling
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Node to Show Profile")
        layout = QVBoxLayout()

        # Add a ComboBox to select the node
        combo_box = QComboBox(dialog)
        combo_box.addItems([node for node in self.dag.nodes if 'dataset_id' in self.dag.nodes[node]])
        layout.addWidget(combo_box)

        # Add a button to show the profile
        btn_show = QPushButton("Show Profile", dialog)
        btn_show.clicked.connect(lambda: self.display_profile(combo_box.currentText(), dialog))
        layout.addWidget(btn_show)

        dialog.setLayout(layout)
        dialog.exec_()

    def display_profile(self, node_name, dialog):
        dialog.accept()  # Close the dialog

        # Retrieve dataset from the selected node
        node = self.dag.nodes[node_name]
        dataset_id = node.get('dataset_id')

        if dataset_id is not None:
            df = self.run.get_dataset_by_id(dataset_id)  # Assuming run object has this method
            
            # Create a profile using pandas' describe() method
            profile = df.describe(include='all')

            # Display the profile in a new window (or any other way)
            profile_dialog = QDialog(self)
            profile_dialog.setWindowTitle(f"Profile for {node_name}")
            layout = QVBoxLayout()

            # Create a label to display profile as text
            profile_label = QLabel(str(profile.to_string()))
            layout.addWidget(profile_label)

            profile_dialog.setLayout(layout)
            profile_dialog.exec_()
        else:
            QMessageBox.warning(self, "Error", f"No dataset found for node {node_name}")

    def compare_profile(self):
        pass

    def save_profile(self):
        create_run(self.run)
        QMessageBox.information(self, "Save Profile Run", "Saved!!")

    def load_profile(self):
        # Load PipelineRun object and run from the database
        # Create raw_data and processing_steps from run object
        self.draw_DAG()
        pass


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    # app.setStyle(QStyleFactory.create("gtk"))
    screen = MainUIWindow() 
    screen.show()   
    sys.exit(app.exec_())


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

        # Create a dialog to let the user select a node
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Node to Show Profile")
        layout = QVBoxLayout()

        # Add a ComboBox to select the node
        combo_box = QComboBox(dialog)
        
        # Only add nodes that have a 'dataset_id' after the pipeline is run
        nodes_with_data = [node for node in self.dag.nodes if 'dataset_id' in self.dag.nodes[node]]
        
        # If no nodes with datasets are available, show a warning
        if not nodes_with_data:
            QMessageBox.warning(self, "Warning", "No datasets available to profile.")
            return

        combo_box.addItems(nodes_with_data)
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
            try:
                # Use the correct method from your PipelineRun class to get the dataset
                df = self.run.get_dataset(dataset_id)
                
                if df is None or df.empty:
                    QMessageBox.warning(self, "Error", "The dataset is empty or could not be retrieved.")
                    return
                
                # Create a profile using pandas' describe() method
                profile = df.describe(include='all')

                # Display the profile in a new dialog
                profile_dialog = QDialog(self)
                profile_dialog.setWindowTitle(f"Profile for {node_name}")
                layout = QVBoxLayout()

                # Create a QLabel to display the profile
                profile_label = QLabel(profile.to_string())
                profile_label.setTextInteractionFlags(Qt.TextSelectableByMouse)  # Make text selectable
                layout.addWidget(profile_label)

                profile_dialog.setLayout(layout)
                profile_dialog.exec_()

            except Exception as e:
                QMessageBox.critical(self, "Error", f"An error occurred while retrieving the dataset: {str(e)}")
        else:
            QMessageBox.warning(self, "Error", f"No dataset found for node {node_name}")

    def compare_profile(self):
        if self.run is None:
           QMessageBox.warning(self, "Warning", "No pipeline has been run yet.")
           return

    # Open a dialog to select two nodes for comparison
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Nodes to Compare")
        layout = QVBoxLayout()

    # Add two ComboBoxes to let the user select input and output nodes
        combo_box_1 = QComboBox(dialog)
        combo_box_1.addItems([node for node in self.dag.nodes if 'dataset_id' in self.dag.nodes[node]])
        layout.addWidget(QLabel("Select First Node (Input)"))
        layout.addWidget(combo_box_1)

        combo_box_2 = QComboBox(dialog)
        combo_box_2.addItems([node for node in self.dag.nodes if 'dataset_id' in self.dag.nodes[node]])
        layout.addWidget(QLabel("Select Second Node (Output)"))
        layout.addWidget(combo_box_2)

    # Add the compare button
        btn_compare = QPushButton("Compare Profiles", dialog)
        btn_compare.clicked.connect(lambda: self.display_profile_comparison(combo_box_1.currentText(), combo_box_2.currentText(), dialog))
        layout.addWidget(btn_compare)

        dialog.setLayout(layout)
        dialog.exec_()

    def display_profile_comparison(self, node_name_1, node_name_2, dialog):
        dialog.accept()  

    # Get datasets from the two selected nodes
        node_1 = self.dag.nodes[node_name_1]
        dataset_id_1 = node_1.get('dataset_id')

        node_2 = self.dag.nodes[node_name_2]
        dataset_id_2 = node_2.get('dataset_id')

        if dataset_id_1 is not None and dataset_id_2 is not None:
           df_1 = self.run.get_dataset_by_id(dataset_id_1)
           df_2 = self.run.get_dataset_by_id(dataset_id_2)

        # Generate statistical summaries (profiles)
           profile_1 = df_1.describe(include='all')
           profile_2 = df_2.describe(include='all')

        # Calculate the differences in statistical measures
           profile_diff = profile_2 - profile_1

        # Display the comparison results
           self.show_comparison_result(profile_1, profile_2, profile_diff, node_name_1, node_name_2)
        else:
            QMessageBox.warning(self, "Error", "One or both datasets are missing.")

    def show_comparison_result(self, profile_1, profile_2, profile_diff, node_name_1, node_name_2):
    # Create a dialog to display the comparison result
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Profile Comparison: {node_name_1} vs {node_name_2}")
        layout = QVBoxLayout()

    # Show the profile comparison result
        comparison_text = f"--- Profile of {node_name_1} ---\n{profile_1.to_string()}\n\n"
        comparison_text += f"--- Profile of {node_name_2} ---\n{profile_2.to_string()}\n\n"
        comparison_text += f"--- Differences ---\n{profile_diff.to_string()}"

        label = QLabel(comparison_text)
        layout.addWidget(label)

        dialog.setLayout(layout)
        dialog.exec_()
       


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

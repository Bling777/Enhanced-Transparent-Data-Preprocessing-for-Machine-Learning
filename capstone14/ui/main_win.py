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
from csv import DictReader

from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.ui.add_process_step import AddProcessStepWin
from capstone14.ui.data_trans_type import DataTransType, run_data_transformation
from capstone14.db.db_functions import create_run, get_available_runs, db
from capstone14.data_logging.functions import save_pipeline_run_to_file

from PyQt5.QtWidgets import QDialog, QVBoxLayout, QTableWidget, QTableWidgetItem, QLabel, QPushButton
from PyQt5.QtGui import QColor
from PyQt5.QtWebEngineWidgets import QWebEngineView
import json
import numpy as np
from datetime import datetime

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
                   ('Compare Profiles', self.compare_profiles), 
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
                fieldnames = []
                with open(file_path, 'r') as infile:
                    reader = DictReader(infile)
                    fieldnames = reader.fieldnames
                # print(fieldnames)

                self.dag.add_node(name, id=id, type='raw', fields=fieldnames, path=file_path, description=file_desc)
                self.draw_DAG()  # Update DAG display
    
    def add_pstep(self):
        AddProcessStepWin.set_dag_and_show(self.dag)
        self.draw_DAG()  # Update DAG display

    def run_pipeline(self):
        if self.dag.number_of_nodes() == 0:
            QMessageBox.warning(self, "Warning", "No data or steps in the preprocessing")
            return

        print("##### Start: run_pipeline #####")

        self.run = None
        self.run = PipelineRun()

        # iterate according to the postion of nodes
        for node_generation in [sorted(generation) for generation in nx.topological_generations(self.dag)]:
            for node_name in node_generation:
                print(f"### Read {node_name} ###")
                node = self.dag.nodes[node_name]
                if node['type'] == 'raw': # if the node is a raw data
                    df = read_csv(node['path'])
                    df.fillna(np.nan, inplace=True) # for KNNImputer
                    node['dataset_id'] = self.run.add_dataset(df)

                elif node['type'] == 'step': # if the node is a data transformation step
                    input_dataset_ids = []
                    # iterate the input nodes of the node
                    for input_node_name in sorted(nx.ancestors(self.dag, node_name)):
                        input_node = self.dag.nodes[input_node_name]
                        
                        # check if the input node is an adjacent node (includes only immediately proceeding nodes)
                        if node_name in list(self.dag.adj[input_node_name]):
                            input_dataset_ids.append(input_node['dataset_id'])
                    out_dataset_id = run_data_transformation(self.run, node['trans_type'], input_dataset_ids,
                                                                 node['ref_fields_1'], node['ref_fields_2'])
                    if out_dataset_id is None:
                        QMessageBox.warning(self, "Warning", f"{node_name} produces no data. Stop running!")
                        return
                    node['dataset_id'] = out_dataset_id

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
   

    def compare_profiles(self): 
        if self.run is None:
            QMessageBox.warning(self, "Warning", "No pipeline has been run yet.")
            return

        # Create dialog for node selection
        dialog = QDialog(self)
        dialog.setWindowTitle("Select Nodes to Compare")
        layout = QVBoxLayout()

        # First node selection (input files)
        layout.addWidget(QLabel("Select First Node (Input - Uploaded Files Only)"))
        combo_box_1 = QComboBox(dialog)
        # Get all nodes that start with 'R' (raw input files)
        raw_files = [node for node in self.dag.nodes 
                     if 'dataset_id' in self.dag.nodes[node] 
                     and node.startswith('R')]
        combo_box_1.addItem("")  # Add empty item as default
        if raw_files:
            combo_box_1.addItems(raw_files)
        layout.addWidget(combo_box_1)

        # Second node selection (processed files)
        layout.addWidget(QLabel("Select Second Node (Output)"))
        combo_box_2 = QComboBox(dialog)
        combo_box_2.addItems([])  # Initialize empty
        layout.addWidget(combo_box_2)

        # Update second combo box when first selection changes
        def update_second_combo():
            combo_box_2.clear()
            selected_input = combo_box_1.currentText()
            if selected_input:
                # Get all descendant nodes that start with 'S' (processed steps)
                processed_nodes = []
                for node in self.dag.nodes:
                    if (node.startswith('S') and 
                        'dataset_id' in self.dag.nodes[node] and 
                        nx.has_path(self.dag, selected_input, node)):
                        processed_nodes.append(node)
                combo_box_2.addItems(processed_nodes)

        combo_box_1.currentTextChanged.connect(update_second_combo)

        # Add Compare button
        btn_compare = QPushButton("Compare Profiles", dialog)
        def do_comparison():
            if not combo_box_1.currentText():
                QMessageBox.warning(dialog, "Warning", "Please select an input node.")
                return
            if not combo_box_2.currentText():
                QMessageBox.warning(dialog, "Warning", "Please select an output node.")
                return
            self.display_profile_comparison(
                combo_box_1.currentText(), 
                combo_box_2.currentText(), 
                dialog
            )
        btn_compare.clicked.connect(do_comparison)
        layout.addWidget(btn_compare)

        # Add Cancel button
        btn_cancel = QPushButton("Cancel", dialog)
        btn_cancel.clicked.connect(dialog.reject)
        layout.addWidget(btn_cancel)

        dialog.setLayout(layout)
        dialog.exec_()

    
        dialog.accept()  # Close the selection dialog

        # Get datasets
        node_1 = self.dag.nodes[node_name_1]
        node_2 = self.dag.nodes[node_name_2]
        
        dataset_id_1 = node_1.get('dataset_id')
        dataset_id_2 = node_2.get('dataset_id')

        if dataset_id_1 is None or dataset_id_2 is None:
            QMessageBox.warning(self, "Error", "One or both datasets are missing.")
            return

        try:
            # Get the datasets from pipeline run
            df_1 = self.run.get_dataset(dataset_id_1)
            df_2 = self.run.get_dataset(dataset_id_2)

            if df_1 is None or df_2 is None:
                QMessageBox.warning(self, "Error", "Unable to retrieve one or both datasets.")
                return

            # Generate profiles
            profile_1 = df_1.describe(include='all')
            profile_2 = df_2.describe(include='all')

            # Calculate differences for numeric columns only
            numeric_cols = df_1.select_dtypes(include=['int64', 'float64']).columns
            profile_diff = profile_2[numeric_cols] - profile_1[numeric_cols]

            # Show comparison results
            self.show_comparison_result(profile_1, profile_2, profile_diff, node_name_1, node_name_2)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred during comparison: {str(e)}")

    #def display_profile_comparison(self, node_name_1, node_name_2, dialog):
        dialog.accept()  # Close the selection dialog
        
        try:
            # Get and validate datasets
            node_1 = self.dag.nodes[node_name_1]
            node_2 = self.dag.nodes[node_name_2]
            dataset_id_1 = node_1.get('dataset_id')
            dataset_id_2 = node_2.get('dataset_id')

            if None in (dataset_id_1, dataset_id_2):
                QMessageBox.warning(self, "Error", "One or both datasets are missing.")
                return

            df_1 = self.run.get_dataset(dataset_id_1)
            df_2 = self.run.get_dataset(dataset_id_2)

            if df_1 is None or df_2 is None or df_1.empty or df_2.empty:
                QMessageBox.warning(self, "Error", "Unable to retrieve one or both datasets.")
                return

            # Calculate basic statistics
            numeric_cols = df_1.select_dtypes(include=['int64', 'float64']).columns
            profile_1 = df_1[numeric_cols].describe()
            profile_2 = df_2[numeric_cols].describe()
            profile_diff = profile_2 - profile_1

            # Create main dialog
            result_dialog = QDialog(self)
            result_dialog.setWindowTitle(f"Enhanced Profile Comparison: {node_name_1} vs {node_name_2}")
            main_layout = QVBoxLayout()

            # Create tab widget for different views
            tab_widget = QTabWidget()
            
            # === Tab 1: Summary View ===
            summary_tab = QWidget()
            summary_layout = QVBoxLayout()
            
            # Add scroll area for summary
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll_content = QWidget()
            scroll_layout = QVBoxLayout()
            
            # Add summary statistics
            summary_text = self._generate_summary_statistics(df_1, df_2, profile_diff)
            summary_label = QLabel(summary_text)
            summary_label.setWordWrap(True)
            scroll_layout.addWidget(summary_label)
            
            # Add visualization
            figure = plt.figure(figsize=(10, 6))
            canvas = FigureCanvas(figure)
            self._create_comparison_plots(figure, profile_1, profile_2, numeric_cols)
            scroll_layout.addWidget(canvas)
            
            scroll_content.setLayout(scroll_layout)
            scroll.setWidget(scroll_content)
            summary_layout.addWidget(scroll)
            
            summary_tab.setLayout(summary_layout)
            tab_widget.addTab(summary_tab, "Summary")

            # === Tab 2: Detailed Comparison ===
            comparison_tab = QWidget()
            comparison_layout = QVBoxLayout()
            
            # Add filter controls with only Column and Statistic
            filter_widget = self._create_filter_widget(profile_diff)
            comparison_layout.addWidget(filter_widget)
            
            # Create table container
            table_container = QWidget()
            table_layout = QVBoxLayout()
            
            # Create and populate table
            self.comparison_table = self._create_comparison_table(profile_diff)
            table_layout.addWidget(self.comparison_table)
            
            table_container.setLayout(table_layout)
            comparison_layout.addWidget(table_container)
            
            comparison_tab.setLayout(comparison_layout)
            tab_widget.addTab(comparison_tab, "Detailed Comparison")

            # === Tab 3: Statistical Analysis ===
            stats_tab = QWidget()
            stats_layout = QVBoxLayout()
            
            stats_text = self._perform_statistical_analysis(df_1, df_2, numeric_cols)
            stats_label = QLabel(stats_text)
            stats_label.setWordWrap(True)
            stats_layout.addWidget(stats_label)
            
            stats_tab.setLayout(stats_layout)
            tab_widget.addTab(stats_tab, "Statistical Analysis")

            # Add export button
            export_btn = QPushButton("Export Results")
            export_btn.clicked.connect(lambda: self._export_results(profile_diff, summary_text, stats_text))

            # Add widgets to main layout
            main_layout.addWidget(tab_widget)
            main_layout.addWidget(export_btn)

            # Set dialog layout and properties
            result_dialog.setLayout(main_layout)
            result_dialog.setMinimumSize(1000, 600)  # Set minimum size
            result_dialog.setSizeGripEnabled(True)    # Allow resizing
            result_dialog.exec_()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred during comparison: {str(e)}")

    def display_profile_comparison(self, node_name_1, node_name_2, dialog):
        dialog.accept()  # Close the selection dialog
        
        try:
            # Get and validate datasets
            node_1 = self.dag.nodes[node_name_1]
            node_2 = self.dag.nodes[node_name_2]
            dataset_id_1 = node_1.get('dataset_id')
            dataset_id_2 = node_2.get('dataset_id')

            if None in (dataset_id_1, dataset_id_2):
                QMessageBox.warning(self, "Error", "One or both datasets are missing.")
                return

            df_1 = self.run.get_dataset(dataset_id_1)
            df_2 = self.run.get_dataset(dataset_id_2)

            if df_1 is None or df_2 is None or df_1.empty or df_2.empty:
                QMessageBox.warning(self, "Error", "Unable to retrieve one or both datasets.")
                return

            # Calculate basic statistics
            numeric_cols = df_1.select_dtypes(include=['int64', 'float64']).columns
            profile_1 = df_1[numeric_cols].describe()
            profile_2 = df_2[numeric_cols].describe()
            profile_diff = profile_2 - profile_1

            # Create main dialog
            result_dialog = QDialog(self)
            result_dialog.setWindowTitle(f"Enhanced Profile Comparison: {node_name_1} vs {node_name_2}")
            main_layout = QVBoxLayout()

            # Create tab widget for different views
            tab_widget = QTabWidget()
            
            # === Tab 1: Summary View ===
            summary_tab = QWidget()
            summary_layout = QVBoxLayout()
            
            # Add scroll area for summary
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll_content = QWidget()
            scroll_layout = QVBoxLayout()
            
            # Add summary statistics
            summary_text = self._generate_summary_statistics(df_1, df_2, profile_diff)
            summary_label = QLabel(summary_text)
            summary_label.setWordWrap(True)
            scroll_layout.addWidget(summary_label)
            
            # Add visualization
            figure = plt.figure(figsize=(10, 6))
            canvas = FigureCanvas(figure)
            self._create_comparison_plots(figure, profile_1, profile_2, numeric_cols)
            scroll_layout.addWidget(canvas)
            
            scroll_content.setLayout(scroll_layout)
            scroll.setWidget(scroll_content)
            summary_layout.addWidget(scroll)
            
            summary_tab.setLayout(summary_layout)
            tab_widget.addTab(summary_tab, "Summary")

            # === Tab 2: Detailed Comparison ===
            comparison_tab = QWidget()
            comparison_layout = QVBoxLayout()
            
            # Add filter controls with only Column and Statistic
            filter_widget = self._create_filter_widget(profile_diff)
            comparison_layout.addWidget(filter_widget)
            
            # Create table container
            table_container = QWidget()
            table_layout = QVBoxLayout()
            
            # Create and populate table
            self.comparison_table = self._create_comparison_table(profile_diff)
            table_layout.addWidget(self.comparison_table)
            
            table_container.setLayout(table_layout)
            comparison_layout.addWidget(table_container)
            
            comparison_tab.setLayout(comparison_layout)
            tab_widget.addTab(comparison_tab, "Detailed Comparison")

            # === Tab 3: Statistical Analysis ===
            stats_tab = QWidget()
            stats_layout = QVBoxLayout()
            
            # Create scroll area for statistical analysis
            stats_scroll = QScrollArea()
            stats_scroll.setWidgetResizable(True)
            stats_content = QWidget()
            stats_content_layout = QVBoxLayout()
            
            # Add statistical analysis text
            stats_text = self._perform_statistical_analysis(df_1, df_2, numeric_cols)
            stats_label = QLabel(stats_text)
            stats_label.setWordWrap(True)
            stats_content_layout.addWidget(stats_label)
            
            # Set up scroll area
            stats_content.setLayout(stats_content_layout)
            stats_scroll.setWidget(stats_content)
            stats_layout.addWidget(stats_scroll)
            
            stats_tab.setLayout(stats_layout)
            tab_widget.addTab(stats_tab, "Statistical Analysis")

            # Add export button
            export_btn = QPushButton("Export Results")
            export_btn.clicked.connect(lambda: self._export_results(profile_diff, summary_text, stats_text))

            # Add widgets to main layout
            main_layout.addWidget(tab_widget)
            main_layout.addWidget(export_btn)

            # Set dialog layout and properties
            result_dialog.setLayout(main_layout)
            result_dialog.setMinimumSize(1000, 600)  # Set minimum size
            result_dialog.setSizeGripEnabled(True)    # Allow resizing
            result_dialog.exec_()
        
        except Exception as e:
            QMessageBox.critical(self, "Error", f"An error occurred during comparison: {str(e)}")

    def _create_filter_widget(self, profile_diff):
        """Create widget for filtering the comparison results"""
        filter_widget = QWidget()
        filter_layout = QHBoxLayout()
        
        # Column filter
        column_label = QLabel("Column:")
        self.column_combo = QComboBox()
        self.column_combo.addItems(['All'] + list(profile_diff.columns))
        self.column_combo.currentTextChanged.connect(self._apply_filters)
        
        # Statistic filter
        stat_label = QLabel("Statistic:")
        self.stat_combo = QComboBox()
        self.stat_combo.addItems(['All'] + list(profile_diff.index))
        self.stat_combo.currentTextChanged.connect(self._apply_filters)
        
        # Add widgets to layout
        filter_layout.addWidget(column_label)
        filter_layout.addWidget(self.column_combo)
        filter_layout.addWidget(stat_label)
        filter_layout.addWidget(self.stat_combo)
        filter_layout.addStretch()
        
        filter_widget.setLayout(filter_layout)
        return filter_widget

    def _apply_filters(self):
        """Apply filters to the comparison table"""
        if not hasattr(self, 'comparison_table'):
            return
            
        selected_column = self.column_combo.currentText()
        selected_stat = self.stat_combo.currentText()
        
        # Show all rows and columns first
        for row in range(self.comparison_table.rowCount()):
            self.comparison_table.showRow(row)
        for col in range(self.comparison_table.columnCount()):
            self.comparison_table.showColumn(col)
        
        # Apply column filter
        if selected_column != 'All':
            for col in range(self.comparison_table.columnCount()):
                if self.comparison_table.horizontalHeaderItem(col).text() != selected_column:
                    self.comparison_table.hideColumn(col)
        
        # Apply statistic filter
        if selected_stat != 'All':
            for row in range(self.comparison_table.rowCount()):
                if self.comparison_table.verticalHeaderItem(row).text() != selected_stat:
                    self.comparison_table.hideRow(row)
   
   # Add these methods to your MainUIWindow class:

    def _generate_summary_statistics(self, df_1, df_2, profile_diff):
        """Generate summary statistics text comparing the two datasets"""
        summary = []
        summary.append("=== Dataset Overview ===")
        summary.append(f"Dataset 1 Shape: {df_1.shape}")
        summary.append(f"Dataset 2 Shape: {df_2.shape}")
        
        # Column comparison
        common_cols = set(df_1.columns) & set(df_2.columns)
        only_df1 = set(df_1.columns) - set(df_2.columns)
        only_df2 = set(df_2.columns) - set(df_1.columns)
        
        summary.append("\n=== Column Analysis ===")
        summary.append(f"Common columns: {len(common_cols)}")
        if only_df1:
            summary.append(f"Columns only in first dataset: {', '.join(only_df1)}")
        if only_df2:
            summary.append(f"Columns only in second dataset: {', '.join(only_df2)}")
        
        # Numeric column changes
        summary.append("\n=== Major Changes in Numeric Columns ===")
        for col in profile_diff.columns:
            mean_change = profile_diff.loc['mean', col]
            std_change = profile_diff.loc['std', col]
            summary.append(f"\n{col}:")
            summary.append(f"  Mean change: {mean_change:.2f}")
            summary.append(f"  Std deviation change: {std_change:.2f}")
        
        return '\n'.join(summary)
   
    def _create_comparison_plots(self, figure, profile_1, profile_2, numeric_cols):
        """Create comparison plots for numeric columns with improved layout"""
        figure.clear()
        n_cols = len(numeric_cols)
        if n_cols == 0:
            return
        
        max_plots = min(n_cols, 9)  
        
        if max_plots <= 3:
            n_rows, n_plot_cols = 1, max_plots
        elif max_plots <= 6:
            n_rows, n_plot_cols = 2, (max_plots + 1) // 2
        else:  
            n_rows, n_plot_cols = 3, 3
        
        figure.set_size_inches(5 * n_plot_cols, 4 * n_rows)
        
        for idx, col in enumerate(list(numeric_cols)[:max_plots], 1):
            ax = figure.add_subplot(n_rows, n_plot_cols, idx)
            
            means = [profile_1.loc['mean', col], profile_2.loc['mean', col]]
            stds = [profile_1.loc['std', col], profile_2.loc['std', col]]
            
            bars = ax.bar(['Dataset 1', 'Dataset 2'], means, yerr=stds, 
                        capsize=5, color=['#2196F3', '#4CAF50'],
                        alpha=0.7)
            
            ax.set_title(f'{col}', pad=10, fontsize=10)
            ax.set_ylabel('Value', fontsize=9)
            
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}',
                    ha='center', va='bottom', fontsize=8)

            ax.tick_params(axis='both', labelsize=8)
            plt.setp(ax.get_xticklabels(), rotation=45)
            
            ax.grid(True, linestyle='--', alpha=0.7)
            
            ymin, ymax = ax.get_ylim()
            ax.set_ylim(ymin, ymax * 1.1)  
        
        figure.tight_layout(pad=3.0)
   
    def _create_comparison_table(self, profile_diff):
        """Create a table widget showing the comparison results"""
        table = QTableWidget()
        table.setRowCount(len(profile_diff.index))
        table.setColumnCount(len(profile_diff.columns))
        
        # Set headers
        table.setHorizontalHeaderLabels(profile_diff.columns)
        table.setVerticalHeaderLabels(profile_diff.index)
        
        # Populate cells
        for i in range(len(profile_diff.index)):
            for j in range(len(profile_diff.columns)):
                value = profile_diff.iloc[i, j]
                item = QTableWidgetItem(f"{value:.4f}")
                
                # Color code cells based on value
                if abs(value) > 0:
                    color = QColor(255, 200, 200) if value < 0 else QColor(200, 255, 200)
                    item.setBackground(color)
                
                table.setItem(i, j, item)
        
        # Adjust table properties
        table.resizeColumnsToContents()
        table.resizeRowsToContents()
        
        return table

    def _perform_statistical_analysis(self, df_1, df_2, numeric_cols):
        """Perform statistical analysis on the two datasets"""
        analysis = []
        analysis.append("=== Statistical Analysis ===\n")
        
        for col in numeric_cols:
            if col in df_1.columns and col in df_2.columns:
                analysis.append(f"\nColumn: {col}")
                
                # Basic statistics
                mean_diff = df_2[col].mean() - df_1[col].mean()
                std_diff = df_2[col].std() - df_1[col].std()
                
                analysis.append(f"Mean difference: {mean_diff:.4f}")
                analysis.append(f"Standard deviation difference: {std_diff:.4f}")
                
                # Data range
                range_1 = df_1[col].max() - df_1[col].min()
                range_2 = df_2[col].max() - df_2[col].min()
                range_diff = range_2 - range_1
                
                analysis.append(f"Range difference: {range_diff:.4f}")
                
                # Check for potential outliers
                q1_1, q3_1 = df_1[col].quantile([0.25, 0.75])
                q1_2, q3_2 = df_2[col].quantile([0.25, 0.75])
                iqr_1 = q3_1 - q1_1
                iqr_2 = q3_2 - q1_2
                
                outliers_1 = ((df_1[col] < (q1_1 - 1.5 * iqr_1)) | (df_1[col] > (q3_1 + 1.5 * iqr_1))).sum()
                outliers_2 = ((df_2[col] < (q1_2 - 1.5 * iqr_2)) | (df_2[col] > (q3_2 + 1.5 * iqr_2))).sum()
                
                analysis.append(f"Potential outliers in dataset 1: {outliers_1}")
                analysis.append(f"Potential outliers in dataset 2: {outliers_2}")
        
        return '\n'.join(analysis)
    
    def display_statistical_analysis(self, analysis_text):
        # Create the dialog for displaying statistical analysis
        dialog = QDialog(self)
        dialog.setWindowTitle("Enhanced Profile Comparison")

        # Create the tab widget and add tabs
        tabs = QTabWidget()
        
        # Create the statistical analysis tab with scroll functionality
        analysis_tab = QWidget()
        analysis_layout = QVBoxLayout()

        # Add a QLabel with the statistical analysis text
        analysis_label = QLabel(analysis_text)
        analysis_label.setWordWrap(True)

        # Create a scroll area for the analysis content
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(analysis_label)

        # Add scroll area to the layout
        analysis_layout.addWidget(scroll_area)
        analysis_tab.setLayout(analysis_layout)

        # Add the tab to the tab widget
        tabs.addTab(analysis_tab, "Statistical Analysis")

        # Create layout for dialog and add widgets
        main_layout = QVBoxLayout()
        main_layout.addWidget(tabs)

        # Add export button
        export_button = QPushButton("Export Results")
        main_layout.addWidget(export_button)

        dialog.setLayout(main_layout)
        dialog.resize(800, 600)  # Set dialog size to fit content and add scroll functionality
        dialog.exec_()

    def _export_results(self, profile_diff, summary_text, stats_text):
        """Export comparison results to a file"""
        file_path, _ = QFileDialog.getSaveFileName(self, 'Save Comparison Results', '', 'Text Files (*.txt)')
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write("=== Profile Comparison Results ===\n\n")
                    f.write("--- Summary ---\n")
                    f.write(summary_text)
                    f.write("\n\n--- Statistical Analysis ---\n")
                    f.write(stats_text)
                    f.write("\n\n--- Detailed Profile Differences ---\n")
                    f.write(profile_diff.to_string())
                QMessageBox.information(self, "Export Successful", "Results have been exported successfully!")
            except Exception as e:
                QMessageBox.critical(self, "Export Error", f"An error occurred while exporting: {str(e)}")
    
    
            
    def save_profile(self):
        if self.run is not None:
            #create_run(self.run)
            save_pipeline_run_to_file(self.run, ".")
            QMessageBox.information(self, "Save Profile to File", "Saved!!")
        else:
            QMessageBox.warning(self, "Warning", "You should 'Run Pipeline' first!")

    def load_profile(self):
        """
        Load a pipeline profile from the database and reconstruct the DAG visualization.
        Shows a dialog with run details and handles errors gracefully.
        """
        try:
            # Get available runs from database
            available_runs = get_available_runs()
            
            if not available_runs:
                QMessageBox.warning(self, "Warning", "No saved pipeline profiles found in the database.")
                return
                
            # Create and configure the dialog
            dialog = QDialog(self)
            dialog.setWindowTitle("Load Pipeline Profile")
            dialog.setMinimumWidth(500)
            layout = QVBoxLayout()
            
            # Add descriptive label
            layout.addWidget(QLabel("Select a pipeline run to load:"))
            
            # Create table widget for better run visualization
            table = QTableWidget()
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(["Run ID", "Start Time", "Datasets", "Processing Steps"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
            
            # Convert all start_times to strings for comparison
            for run in available_runs:
                if isinstance(run['start_time'], datetime):
                    run['start_time_str'] = run['start_time'].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    run['start_time_str'] = str(run['start_time'])
            
            # Sort runs by start_time_str
            sorted_runs = sorted(available_runs, 
                            key=lambda x: x['start_time_str'],
                            reverse=True)
            
            # Populate table with run data
            table.setRowCount(len(sorted_runs))
            for row, run in enumerate(sorted_runs):
                # Run ID
                id_item = QTableWidgetItem(str(run['run_id']))
                table.setItem(row, 0, id_item)
                
                # Start Time - use the string version
                time_item = QTableWidgetItem(run['start_time_str'])
                table.setItem(row, 1, time_item)
                
                # Dataset Count
                dataset_count = len(run.get('dataset_ids', []))
                dataset_item = QTableWidgetItem(str(dataset_count))
                table.setItem(row, 2, dataset_item)
                
                # Processing Steps Count
                step_count = len(run.get('processing_steps', []))
                step_item = QTableWidgetItem(str(step_count))
                table.setItem(row, 3, step_item)
                
                # Store the full run data in the first column item
                id_item.setData(Qt.UserRole, run)
            
            layout.addWidget(table)
            
            # Add load and cancel buttons in a horizontal layout
            button_layout = QHBoxLayout()
            btn_load = QPushButton("Load Selected Profile")
            btn_cancel = QPushButton("Cancel")
            button_layout.addWidget(btn_load)
            button_layout.addWidget(btn_cancel)
            layout.addLayout(button_layout)
            
            # Connect buttons to actions
            btn_cancel.clicked.connect(dialog.reject)
            btn_load.clicked.connect(lambda: self._load_selected_profile(
                table.item(table.currentRow(), 0).data(Qt.UserRole) if table.currentRow() >= 0 else None,
                dialog
            ))
            
            # Select first row by default
            if table.rowCount() > 0:
                table.setCurrentCell(0, 0)
            
            dialog.setLayout(layout)
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load pipeline profiles: {str(e)}")

    #def _load_selected_profile(self, run_data: dict, dialog: QDialog):
      

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    # app.setStyle(QStyleFactory.create("gtk"))
    screen = MainUIWindow() 
    screen.show()   
    sys.exit(app.exec_())

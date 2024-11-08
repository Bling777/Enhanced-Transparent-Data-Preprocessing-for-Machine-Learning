from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import networkx as nx
import pandas as pd
import os

# Set environment variables for Qt scaling
os.environ["QT_AUTO_SCREEN_SCALE_FACTOR"] = "1"
os.environ["QT_SCALE_FACTOR"] = "1"
os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"

from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.ui.add_process_step import AddProcessStepWin
from capstone14.data_profiling.data_profile import DataProfile


class MainUIWindow(QWidget):

    def __init__(self):
        super(MainUIWindow, self).__init__()        
        self.raw_data = []  
        self.processing_steps = []  
        self.run = None  
        font = QFont()
        font.setPointSize(16)
        self.initUI()

    def initUI(self):
        self.setGeometry(100, 100, 800, 600)
        self.center()
        self.setWindowTitle('Transparent Data Preprocessing System')

        grid = QGridLayout()
        self.setLayout(grid)
        self.createHGroupBox() 

        buttonLayout = QHBoxLayout()
        buttonLayout.addWidget(self.horizontalGroupBox)
        grid.addLayout(buttonLayout, 0, 0)

        self.figure = plt.figure()
        self.canvas = FigureCanvas(self.figure)        
        grid.addWidget(self.canvas, 1, 0, 9, 9)          

        self.add_pstep = AddProcessStepWin()

    def center(self):
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    def createHGroupBox(self):
        self.horizontalGroupBox = QGroupBox()

        layout = QHBoxLayout()

        button = QPushButton('Add Raw Data', self)
        button.clicked.connect(self.add_raw_data)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Add Step', self)
        button.clicked.connect(self.add_pstep)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Run Pipeline', self)
        button.clicked.connect(self.run_pipeline)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Show Profile', self)
        button.clicked.connect(self.show_profile)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Compare Profiles', self)
        button.clicked.connect(self.compare_profile)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Save Pipeline', self)
        button.clicked.connect(self.save_profile)
        layout.addWidget(button)
        layout.setSpacing(10)

        button = QPushButton('Load Pipeline', self)
        button.clicked.connect(self.load_profile)
        layout.addWidget(button)

        self.horizontalGroupBox.setLayout(layout)

    def draw_DAG(self):
        G = nx.DiGraph(
            [
                ("f", "a"),
                ("a", "b"),
                ("a", "e"),
                ("b", "c"),
                ("b", "d"),
                ("d", "e"),
                ("f", "c"),
                ("f", "g"),
                ("h", "f"),
            ]
        )

        for layer, nodes in enumerate(nx.topological_generations(G)):
            for node in nodes:
                G.nodes[node]["layer"] = layer

        pos = nx.multipartite_layout(G, subset_key="layer")
        nx.draw(G, pos=pos, with_labels=True)
        self.canvas.draw_idle()
        self.show()

    def add_raw_data(self):
        # Add raw data file into raw_data list
        fname, _ = QFileDialog.getOpenFileName(self, 'Open Raw File', '', 'CSV Files (*.csv)')
        
        if fname:  # If a file is selected
            raw_data_entry = {
                'id': len(self.raw_data),  # Assign a unique ID
                'description': f'Raw data file {len(self.raw_data) + 1}',  # Simple description
                'filepath': fname  # File path
            }
            self.raw_data.append(raw_data_entry)  # Store in global variable

            self.draw_DAG()  # Update DAG display
        pass

    def add_pstep(self):
        self.add_pstep.raw_data = self.raw_data
        self.add_pstep.processing_steps = self.processing_steps

        # Add processing step
        step = self.add_pstep.get_step()  # Assuming get_step method gets a processing step
        if step:
            self.processing_steps.append(step)  # Add step to the list of processing steps

        self.draw_DAG()  # Update DAG display
        pass

    def run_pipeline(self):
        if not self.raw_data:
            QMessageBox.warning(self, "Warning", "No raw data added!")
            return

      
        try:
            input_data = pd.read_csv(self.raw_data[0]['filepath'])  
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load dataset: {str(e)}")
            return
        
        self.run = PipelineRun(input_data)  
        self.run.execute() 
        QMessageBox.information(self, "Info", "Pipeline executed successfully!")

    def show_profile(self):
        if self.run is None:
            QMessageBox.warning(self, "Warning", "Pipeline has not been executed yet!")
            return

       
        if not self.run.datasets:
            QMessageBox.warning(self, "Warning", "No datasets available!")
            return

        dataset_id = self.run.datasets[0]["id"]  
        profile_data = self.run.get_data_profile_of_dataset(dataset_id)  

        if profile_data is None:
            QMessageBox.warning(self, "Warning", "No profile available for the dataset!")
            return

        # Display the profile using a table or a plot (for example, statistics in a table)
        profile_window = QDialog(self)
        profile_window.setWindowTitle("Data Profile")
        
        layout = QVBoxLayout()
        profile_window.setLayout(layout)
        
       
        profile_text = profile_data.__repr__()
        
        
        stats_label = QLabel(profile_text)
        layout.addWidget(stats_label)
        
        profile_window.exec_()



    def compare_profile(self):
        pass

    def save_profile(self):
        pass

    def load_profile(self):
        # Load PipelineRun object and run from the database
        # Create raw_data and processing_steps from run object
        self.draw_DAG()
        pass


if __name__ == '__main__':
    import sys  
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    app.setStyle(QStyleFactory.create("gtk"))
    screen = MainUIWindow() 
    screen.show()   
    sys.exit(app.exec_())


import sys  
import os
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import networkx as nx

from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.ui.add_process_step import AddProcessStepWin


class MainUIWindow(QWidget):

    def __init__(self):
        super(MainUIWindow, self).__init__()        
        # Initialize raw_data and processing_steps
        self.raw_data = []  # Used to store added raw data files
        self.processing_steps = []  # Used to store processing steps
        font = QFont()
        font.setPointSize(16)
        self.initUI()

        self.dag = nx.DiGraph()


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
        print(self.dag.nodes.data())

        pos = {}
        if (len(self.dag.edges)):
            for layer, nodes in enumerate(nx.topological_generations(self.dag)):
                for node in nodes:
                    self.dag.nodes[node]["layer"] = layer

            pos = nx.multipartite_layout(self.dag, subset_key="layer")
        else:
            for i, nd in enumerate(self.dag.nodes):
                pos[nd] = [0,i]
            print(pos)

        nx.draw(self.dag, pos=pos, with_labels=True, node_shape='s')
        self.canvas.draw_idle()

        # else:
        #     nx.draw_networkx_nodes(self.dag, pos=nx.spring_layout(self.dag), with_labels=True)
        #     self.canvas.draw_idle()
        # self.show()

    def add_raw_data(self):
        # Add raw data file into raw_data list
        fname, _ = QFileDialog.getOpenFileName(self, 'Open Raw File', '', 'CSV Files (*.csv)')
        
        if fname:  # If a file is selected
            # raw_data_entry = {
            #     'id': len(self.raw_data),  # Assign a unique ID
            #     'description': f'Raw data file {len(self.raw_data) + 1}',  # Simple description
            #     'filepath': fname  # File path
            # }
            # self.raw_data.append(raw_data_entry)  # Store in global variable

            file_desc = f'Raw data file {len(self.raw_data) + 1}',  # Simple description
            file_path = fname  # File path
            self.dag.add_node(os.path.basename(file_path)) #, type='raw', description=file_desc, path=file_path, color='bule')

            self.draw_DAG()  # Update DAG display
        pass

    def add_pstep(self):
        self.add_pstep.raw_data = self.raw_data
        self.add_pstep.processing_steps = self.processing_steps
        self.add_pstep.show()

        # Add processing step
        # step = self.add_pstep.get_step()  # Assuming get_step method gets a processing step
        # if step:
        #     self.processing_steps.append(step)  # Add step to the list of processing steps

        self.draw_DAG()  # Update DAG display
        pass

    def run_pipeline(self):
        if not self.raw_data:
            QMessageBox.warning(self, "Warning", "No raw data added!")
            return

        # Assuming PipelineRun handles data processing
        run = PipelineRun(self.raw_data, self.processing_steps)  # Pass raw_data and processing steps
        run.execute()  # Execute the pipeline
        QMessageBox.information(self, "Info", "Pipeline executed successfully!")

    #'run pipeline' UI implementation-Ringo
    # def run_pipeline(self):
    #     if not self.raw_data_path:
    #         QMessageBox.warning(self, "Error", "Please add raw data first.")
    #         return

    #     try:
    #         raw_data = pd.read_csv(self.raw_data_path)
    #         self.pipeline_run = PipelineRun(raw_data)

    #         # Here to can add preprocessing steps

    #         QMessageBox.information(self, "Pipeline Run", f"Pipeline run completed. Run ID: {self.pipeline_run.run_id}")

    #     except Exception as e:
    #         QMessageBox.critical(self, "Error", f"An error occurred while running 

    def show_profile(self):
        pass

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
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    app.setStyle(QStyleFactory.create("gtk"))
    screen = MainUIWindow() 
    screen.show()   
    sys.exit(app.exec_())

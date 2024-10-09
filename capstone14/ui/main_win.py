from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import networkx as nx

from capstone14.data_logging.pipeline_run import PipelineRun
from capstone14.ui.add_process_step import AddProcessStepWin


# Data to display DAG
raw_data = [] # list of {id, description, filepath}
processing_steps = [] # list of dict {id, name, [input_step_ids]} (first steps have raw_data ids as input_step_ids)
run = PipelineRun() # PipelineRun object created by running the DAG preprocessing


class MainUIWindow(QWidget):

    def __init__(self):
        super(MainUIWindow, self).__init__()        
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
        button.clicked.connect(self.save_profile)
        layout.addWidget(button)

        self.horizontalGroupBox.setLayout(layout)

    # add raw datafile to the preprocessing
    def add_raw_data(self):
        # add a raw datafile into raw_data
        fname = QFileDialog.getOpenFileName(self, 'Open Raw File', '', 'csv File(*.csv)')
        self.show_profile()
        pass

    # add a preprocessing step into preprocessing_step
    def add_pstep(self):
        self.add_pstep.raw_data = raw_data
        self.add_pstep.processing_steps = processing_steps
        self.add_pstep.show()
        self.show_profile()
        pass

    # create run object from raw_data and preprocessing_step
    def run_pipeline(self):
        pass

    # draw the DAG on the main window from raw_data and processing_step
    def show_profile(self):
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
            # `multipartite_layout` expects the layer as a node attribute, so add the
            # numeric layer value as a node attribute
            for node in nodes:
                G.nodes[node]["layer"] = layer

        # Compute the multipartite_layout using the "layer" node attribute
        pos = nx.multipartite_layout(G, subset_key="layer")

        nx.draw(G, pos=pos, with_labels=True)
        self.canvas.draw_idle()

        self.show()
        pass

    # calculate difference between input and output columns
    def compare_profile(self):
        pass

    # save run object (PipelineRun) into database
    def save_profile(self):
        pass

    # load run object (PipelineRun) from database
    def load_profile(self):
        # load PipelineRun object, run form database
        # create raw_data processing_steps from run object
        self.show_profile()
        pass


if __name__ == '__main__':

    import sys  
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    app.setStyle(QStyleFactory.create("gtk"))
    screen = MainUIWindow() 
    screen.show()   
    sys.exit(app.exec_())


from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont


class AddProcessStepWin(QWidget):

    def __init__(self):
        super(AddProcessStepWin, self).__init__()        
        font = QFont()
        font.setPointSize(16)
        self.initUI()

        self.raw_data = [] # list of {id, description, filepath}
        self.processing_steps = [] # list of dict {id, name, [input_step_ids]} (first steps have raw_data ids as input_step_ids)

    def initUI(self):
        self.setGeometry(100, 100, 800, 600)
        self.center()
        self.setWindowTitle('Add a Preprocessing Step')

        grid = QGridLayout()
        self.setLayout(grid)
        self.createVGroupBox() 

        listLayout = QVBoxLayout()
        listLayout.addWidget(self.verticalGroupBox)

        grid.addLayout(listLayout, 0, 0)

    def center(self):
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    def createVGroupBox(self):
        self.verticalGroupBox = QGroupBox()

        layout = QVBoxLayout()

        lblProcess = QLabel("Preprocessing Task", self)
        layout.addWidget(lblProcess)
        layout.setSpacing(10)

        listProcess = QListWidget(self)
        layout.addWidget(listProcess)
        layout.setSpacing(10)

        lblProcess = QLabel("Input Data", self)
        layout.addWidget(lblProcess)
        layout.setSpacing(10)

        listInputData = QListWidget(self)
        listInputData.setSelectionMode(QAbstractItemView.MultiSelection)
        layout.addWidget(listInputData)

        QListWidgetItem("Merge", listProcess)
        QListWidgetItem("Deduplicate", listProcess)
        QListWidgetItem("Iimpute Missing Values", listProcess)

        # insert list itmes from raw_data and processing_steps
        QListWidgetItem("RawData1", listInputData)
        QListWidgetItem("RawData2", listInputData)
        QListWidgetItem("Step1", listInputData)
        QListWidgetItem("Step2", listInputData)

        self.verticalGroupBox.setLayout(layout)


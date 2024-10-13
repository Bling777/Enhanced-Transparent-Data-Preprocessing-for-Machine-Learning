from PyQt5.QtCore import QCoreApplication
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont


class AddProcessStepWin(QDialog):
    selected_input_nodes = []
    selected_pstep = ''

    @staticmethod
    def add_process_step(input_items):
        win = AddProcessStepWin()
        for rd in input_items:
            QListWidgetItem(rd, win.inputDataList)
        win.exec_()

    def __init__(self):
        super(AddProcessStepWin, self).__init__()        

        self.raw_data = [] # list of name of raw data
        self.processing_steps = [] # list of name of existing steps

        font = QFont()
        font.setPointSize(16)
        self.initUI()

    def initUI(self):
        self.setGeometry(100, 100, 600, 300)
        self.center()
        self.setWindowTitle('Add a Preprocessing Step')

        # raw data and existing steps <- assigned by main_win.py
        self.inputDataList = QListWidget(self) 
        self.inputDataList.setSelectionMode(QAbstractItemView.MultiSelection)
        self.inputDataList.setSortingEnabled(True)

        # possible processing step
        self.pstepList = QListWidget(self)   
        QListWidgetItem("Merge", self.pstepList)
        QListWidgetItem("Deduplicate", self.pstepList)
        QListWidgetItem("Iimpute Missing Values", self.pstepList)

        btnGroupBox = QGroupBox()
        btnLayout = QVBoxLayout()
        btnAdd = QPushButton('Add Processing Step', self)
        btnAdd.clicked.connect(self.add_raw_data)
        btnCancel = QPushButton('Cancel', self)
        btnCancel.clicked.connect(self.sel_cancel)
        btnLayout.addWidget(btnAdd)
        btnLayout.setSpacing(5)
        btnLayout.addWidget(btnCancel)
        btnGroupBox.setLayout(btnLayout)

        grid = QGridLayout()
        grid.addWidget(QLabel("Input Data", self), 0, 0)
        grid.addWidget(self.inputDataList, 1, 0)
        grid.addWidget(QLabel("Processing Task", self), 0, 1)
        grid.addWidget(self.pstepList, 1, 1)
        grid.addWidget(btnGroupBox, 2, 1)

        self.setLayout(grid)

    def center(self):
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    def add_raw_data(self):
        if len(self.inputDataList.selectedItems()) and len(self.pstepList.selectedItems()):
            AddProcessStepWin.selected_input_nodes = [item.text() for item in self.inputDataList.selectedItems()]
            AddProcessStepWin.selected_pstep = self.pstepList.selectedItems()[0].text()
            self.close()
        else:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText("Please select input data and processing task")
            msg.exec_()

    def sel_cancel(self):
        AddProcessStepWin.selected_input_nodes = []
        AddProcessStepWin.selected_pstep = ''
        self.close()


if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    # app.setStyle(QStyleFactory.create("gtk"))
    screen = AddProcessStepWin() 
    screen.show()   
    sys.exit(app.exec_())

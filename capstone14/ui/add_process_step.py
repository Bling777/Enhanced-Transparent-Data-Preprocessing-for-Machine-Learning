from PyQt5.QtCore import QCoreApplication
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QFont

from capstone14.ui.data_trans_type import DataTransType


class AddProcessStepWin(QDialog):
    selected_input_nodes = []
    selected_pstep = None

    @staticmethod
    def list_input_nodes(input_items):
        win = AddProcessStepWin()
        for rd in input_items:
            QListWidgetItem(rd, win.inputDataList)
        win.exec_()

    def __init__(self):
        super(AddProcessStepWin, self).__init__()        
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Add a Preprocessing Step')
        buttons = (('Add Processing Step', self.add_pstep),
                   ('Cancel', self.sel_cancel))

        grid = QGridLayout()
        self.setLayout(grid)

        # raw data and existing steps <- assigned by main_win.py
        self.inputDataList = QListWidget(self) 
        self.inputDataList.setSelectionMode(QAbstractItemView.MultiSelection)
        self.inputDataList.setSortingEnabled(True)
        grid.addWidget(QLabel("Input Data", self), 0, 0)
        grid.addWidget(self.inputDataList, 1, 0)

        # possible processing step
        self.pstepList = QListWidget(self)  
        for tr in DataTransType:
            QListWidgetItem(tr.value, self.pstepList)
        grid.addWidget(QLabel("Processing Task", self), 0, 1)
        grid.addWidget(self.pstepList, 1, 1)

        # command buttons (add processing step, cancel)
        btnLayout = QVBoxLayout()
        for btn in buttons:
            button = QPushButton(btn[0], self)
            button.clicked.connect(btn[1])
            btnLayout.addWidget(button)
            btnLayout.setSpacing(5)
        btnGroupBox = QGroupBox()
        btnGroupBox.setLayout(btnLayout)
        grid.addWidget(btnGroupBox, 2, 1)

        # set windows size and position (center)
        self.setGeometry(100, 100, 600, 300)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def add_pstep(self):
        inputs = self.inputDataList.selectedItems()
        sel_pstep = self.pstepList.selectedItems()
        
        if len(inputs) == 0 or len(sel_pstep) == 0:
            QMessageBox.warning(self, 'Warning', 'Please select input data and processing task')
            return
        
        pstep = DataTransType(sel_pstep[0].text())
        if (len(inputs) != pstep.num_input):
            QMessageBox.warning(self, 'Warning', f'{pstep.value} needs {pstep.num_input} input dataset(s)')
            return
        
        AddProcessStepWin.selected_input_nodes = [item.text() for item in inputs]
        AddProcessStepWin.selected_pstep = pstep
        self.close()

    def sel_cancel(self):
        AddProcessStepWin.selected_input_nodes = []
        AddProcessStepWin.selected_pstep = None
        self.close()


if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    # app.setStyle(QStyleFactory.create("gtk"))
    screen = AddProcessStepWin() 
    screen.show()   
    sys.exit(app.exec_())

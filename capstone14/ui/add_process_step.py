from PyQt5.QtCore import QCoreApplication
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt

from capstone14.ui.data_trans_type import DataTransType, check_columns


class AddProcessStepWin(QDialog):

    # @staticmethod
    # def list_input_nodes(input_items):
    #     win = AddProcessStepWin()
    #     for rd in input_items:
    #         QListWidgetItem(rd, win.inputDataList)
    #     win.exec_()

    @staticmethod
    def set_dag_and_show(dag):
        win = AddProcessStepWin()
        win.dag = dag
        for rd in list(win.dag.nodes):
            QListWidgetItem(rd, win.inputDataList)
        win.exec_()


    def __init__(self):
        super(AddProcessStepWin, self).__init__()
        self.dag = None
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Add a Preprocessing Step')
        buttons = (('Add Processing Step', self.add_pstep),
                   ('Cancel', self.sel_cancel))

        grid = QGridLayout()
        self.setLayout(grid)

        # possible processing step
        self.pstepList = QListWidget(self)  
        for tr in DataTransType:
            QListWidgetItem(tr.value, self.pstepList)
        grid.addWidget(QLabel("Processing Task", self), 0, 0)
        grid.addWidget(self.pstepList, 1, 0)

        # raw data and existing steps <- assigned by main_win.py
        self.inputDataList = QListWidget(self) 
        self.inputDataList.setSelectionMode(QAbstractItemView.MultiSelection)
        self.inputDataList.setSortingEnabled(True)
        self.inputDataList.selectionModel().selectionChanged.connect(self.display_columns)
        grid.addWidget(QLabel("Input Data", self), 0, 1)
        grid.addWidget(self.inputDataList, 1, 1)

        # column information
        self.columns1_label = QLabel(self)
        self.columns1 = QListWidget(self)  
        self.columns1.setSelectionMode(QAbstractItemView.MultiSelection)
        grid.addWidget(self.columns1_label, 2, 0)
        grid.addWidget(self.columns1, 3, 0)

        self.columns2_label = QLabel(self)
        self.columns2 = QListWidget(self)  
        self.columns2.setSelectionMode(QAbstractItemView.MultiSelection)
        grid.addWidget(self.columns2_label, 2, 1)
        grid.addWidget(self.columns2, 3, 1)

        # command buttons (add processing step, cancel)
        btnLayout = QVBoxLayout()
        for btn in buttons:
            button = QPushButton(btn[0], self)
            button.clicked.connect(btn[1])
            btnLayout.addWidget(button)
            btnLayout.setSpacing(5)
        btnGroupBox = QGroupBox()
        btnGroupBox.setLayout(btnLayout)
        grid.addWidget(btnGroupBox, 4, 1)

        # set windows size and position (center)
        self.setGeometry(100, 100, 600, 600)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def display_columns(self):
        inputs_selected = self.inputDataList.selectedItems()
        if len(inputs_selected) > 0 and self.columns1_label.text() != inputs_selected[0].text():
            self.columns1.clear()
            self.columns1_label.setText(inputs_selected[0].text())
            for col in self.dag.nodes[inputs_selected[0].text()]['fields']:
                QListWidgetItem(col, self.columns1)                
    
        if len(inputs_selected) > 1 and self.columns2_label.text() != inputs_selected[1].text():    
            self.columns2.clear()
            self.columns2_label.setText(inputs_selected[1].text())
            for col in self.dag.nodes[inputs_selected[1].text()]['fields']:
                QListWidgetItem(col, self.columns2)

        if (len(inputs_selected) < 1):
            self.columns1.clear()
            self.columns1_label.setText('')
            self.columns2.clear()
            self.columns2_label.setText('')
        elif (len(inputs_selected) < 2):
            self.columns2.clear()
            self.columns2_label.setText('')

    def add_pstep(self):
        sel_inputs = self.inputDataList.selectedItems()
        sel_pstep = self.pstepList.selectedItems()
        
        if len(sel_inputs) == 0 or len(sel_pstep) == 0:
            QMessageBox.warning(self, 'Warning', 'Please select input data and processing task')
            return
        
        pstep = DataTransType(sel_pstep[0].text())
        if (len(sel_inputs) != pstep.num_input):
            QMessageBox.warning(self, 'Warning', f'{pstep.value} needs {pstep.num_input} input dataset(s)')
            return
        
        cols_1 = [self.columns1.item(x).text() for x in range(self.columns1.count())]
        cols_2 = [self.columns2.item(x).text() for x in range(self.columns2.count())]
        sel_cols_1 = [item.text() for item in self.columns1.selectedItems()]
        sel_cols_2 = [item.text() for item in self.columns2.selectedItems()]

        new_cols = check_columns(pstep, cols_1, cols_2, sel_cols_1, sel_cols_2)
        if new_cols is None:
            QMessageBox.warning(self, 'Warning', 'Please select proper referencing columns')
            return
        print(new_cols)

        id = len(self.dag.nodes)  # Assign a unique ID
        step_name = f'S{id}. {pstep.value}'
        self.dag.add_node(step_name, id=id, type='step', trans_type=pstep, 
                          fields=new_cols, ref_fields_1=sel_cols_1, ref_fields_2=sel_cols_2)

        # for input_nd in AddProcessStepWin.selected_input_nodes:
        for sel_item in sel_inputs:
            self.dag.add_edge(sel_item.text(), step_name)

        self.close()

    def sel_cancel(self):
        self.close()

    def closeEvent(self, event):
        self.dag = None

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self.close()


if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    # app.setStyle(QStyleFactory.create("gtk"))
    screen = AddProcessStepWin() 
    screen.show()   
    sys.exit(app.exec_())

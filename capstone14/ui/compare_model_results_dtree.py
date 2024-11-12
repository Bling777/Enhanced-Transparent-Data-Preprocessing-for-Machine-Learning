from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt
import networkx as nx
from capstone14.ui.display_model_results_dtree import DisplayModelResultsDTreeWin


class CompareModelResultsDTreeWin(QDialog):

    @staticmethod
    def set_run_dag_and_show(run, dag):
        win = CompareModelResultsDTreeWin()
        win.run = run
        win.dag = dag
        for rd in list(win.dag.nodes):
            win.base_node.addItem(rd)
        win.exec_()


    def __init__(self):
        super(CompareModelResultsDTreeWin, self).__init__()
        self.run = None
        self.dag = None
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Compare Decision Tree Results')
        buttons = (('Compare', self.compare),
                   ('Cancel', self.sel_cancel))

        self.base_node = QComboBox(self)
        self.base_node.currentTextChanged.connect(self.display_base_columns_other_nodes)
        base_node_layout = QHBoxLayout()
        base_node_layout.addWidget(QLabel("Base Node: ", self))
        base_node_layout.addWidget(self.base_node)
        base_node_group_box = QGroupBox()
        base_node_group_box.setLayout(base_node_layout)

        # column information for target and data
        self.columns_for_target = QListWidget(self)
        self.columns_for_target.selectionModel().selectionChanged.connect(self.display_data_columns)
        self.columns_for_data = QListWidget(self)
        self.columns_for_data.setSelectionMode(QAbstractItemView.MultiSelection)
        column_layout = QGridLayout()
        column_layout.addWidget(QLabel("Select Target", self), 0, 0)
        column_layout.addWidget(self.columns_for_target, 1, 0)
        column_layout.addWidget(QLabel("Select Data", self), 0, 1)
        column_layout.addWidget(self.columns_for_data, 1, 1)
        column_group_box = QGroupBox()
        column_group_box.setLayout(column_layout)

        self.node_to_compare = QListWidget(self)
        self.node_to_compare.setSelectionMode(QAbstractItemView.MultiSelection)
        node_to_compare_layout = QVBoxLayout()
        node_to_compare_layout.addWidget(QLabel("Nodes to Compare", self))
        node_to_compare_layout.addWidget(self.node_to_compare)
        node_to_compare_group_box = QGroupBox()
        node_to_compare_group_box.setLayout(node_to_compare_layout)

        # command buttons (add processing step, cancel)
        btnLayout = QHBoxLayout()
        for btn in buttons:
            button = QPushButton(btn[0], self)
            button.clicked.connect(btn[1])
            btnLayout.addWidget(button)
            btnLayout.setSpacing(5)
        btnGroupBox = QGroupBox()
        btnGroupBox.setLayout(btnLayout)

        grid = QGridLayout()
        self.setLayout(grid)
        grid.addWidget(base_node_group_box, 0, 0)
        grid.addWidget(column_group_box, 1, 0)
        grid.addWidget(node_to_compare_group_box, 1, 1)
        grid.addWidget(btnGroupBox, 2, 1)

        # set windows size and position (center)
        self.setGeometry(100, 100, 600, 600)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def display_base_columns_other_nodes(self):
        base_node_selected = self.base_node.currentText()
        self.columns_for_target.clear()
        self.columns_for_data.clear()
        for col in self.dag.nodes[base_node_selected]['fields']:
            QListWidgetItem(col, self.columns_for_target)                
    
        self.node_to_compare.clear()
        # for nd_name in sorted(nx.descendants(self.dag, base_node_selected)):
        for nd_name in sorted([x for x in list(self.dag.nodes()) if x != base_node_selected]):
            QListWidgetItem(nd_name, self.node_to_compare)

    def display_data_columns(self):
        targets_selected = self.columns_for_target.selectedItems()
        if len(targets_selected) > 0:
            all_cols = [self.columns_for_target.item(x) for x in range(self.columns_for_target.count())]
            self.columns_for_data.clear()
            for i in [x for x in all_cols if x not in targets_selected]:
                QListWidgetItem(i.text(), self.columns_for_data)                
    
    def compare(self):
        target_col = self.columns_for_target.selectedItems()[0].text()
        data_cols = [x.text() for x in self.columns_for_data.selectedItems()]
        nodes = [self.base_node.currentText()] + [item.text() for item in self.node_to_compare.selectedItems()]
        datasets = []
        for nd_name in nodes:
            node = self.dag.nodes[nd_name]
            datasets.append(self.run.get_dataset(node['dataset_id']))

        DisplayModelResultsDTreeWin.set_dag_nodes_and_show(self.dag, nodes, datasets, target_col, data_cols)

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
    screen = CompareModelResultsDTreeWin() 
    screen.show()   
    sys.exit(app.exec_())

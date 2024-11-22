from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn import tree


class DisplayModelResultsDTreeWin(QDialog):

    @staticmethod
    def set_dag_nodes_and_show(dag, nodes, datasets, target_col, data_cols):
        win = DisplayModelResultsDTreeWin()
        win.dag = dag
        win.node_to_compare = nodes
        win.datasets = datasets
        win.target_col = target_col
        win.data_cols = data_cols
        win.initUI()
        win.exec_()


    def __init__(self):
        super(DisplayModelResultsDTreeWin, self).__init__()
        self.dag = None
        self.node_to_compare = []
        self.datasets = []
        self.target_col = ''
        self.data_cols = []

    def initUI(self):
        self.setWindowTitle('Display Decision Tree Results')

        grid = QGridLayout()
        self.setLayout(grid)

        self.setGeometry(100, 100, 1400, 600)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

        try:        
            clf = DecisionTreeClassifier()
            for i, nd_name in enumerate(self.node_to_compare):
                X = self.datasets[i][self.data_cols]
                y = self.datasets[i][self.target_col]
                # print(X.head())
                # print(y.head())

                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
                # print(X_train.head())
                # print(X_test.head())
                # print(y_train.head())
                # print(y_test.head())

                clf.fit(X_train, y_train)

                y_pred = clf.predict(X_test)
                accuracy = clf.score(X_test, y_test)

                a_layout = QVBoxLayout()
                figure = plt.figure()
                canvas = FigureCanvas(figure)        
                a_layout.addWidget(QLabel(nd_name, self))
                a_layout.addWidget(QLabel(f"Accuracy: {accuracy * 100:.2f}%", self))
                a_layout.addWidget(canvas)
                a_group_box = QGroupBox()
                a_group_box.setLayout(a_layout)
                grid.addWidget(a_group_box, 0, i)
                # tree.plot_tree(clf, filled=True, feature_names=self.data_cols, class_names=[self.target_col])
                tree.plot_tree(clf, filled=True, feature_names=self.data_cols)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"{str(e)}")
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
    screen = DisplayModelResultsDTreeWin() 
    screen.show()   
    sys.exit(app.exec_())

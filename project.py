import sys
from preprocessing import Preprocessing
from interface import GuiInterface
from PySide6 import QtWidgets


if __name__ == "__main__":
    try:
        app = QtWidgets.QApplication([])

        widget = GuiInterface()
        widget.resize(800, 600)
        widget.show()

        sys.exit(app.exec())
    
    finally:
        print('Connection Closed.')
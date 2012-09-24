import sys

import sip
sip.setapi('QVariant', 2)

from PyQt4 import QtCore, QtGui

class SystemTrayIcon(QtGui.QSystemTrayIcon):

    def __init__(self, icon, parent=None):
        QtGui.QSystemTrayIcon.__init__(self, icon, parent)
        menu = QtGui.QMenu(parent)
        exitAction = menu.addAction("Exit")
        self.setContextMenu(menu)

def main():
    app = QtGui.QApplication(sys.argv)
    QtGui.QApplication.setQuitOnLastWindowClosed(False)
    
    w = QtGui.QWidget()
    trayIcon = SystemTrayIcon(QtGui.QIcon("tag.gif"), w)

    trayIcon.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()

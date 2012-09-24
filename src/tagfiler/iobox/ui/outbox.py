import sys

import sip
sip.setapi('QVariant', 2)

from PyQt4 import QtCore, QtGui


class OutboxUI():
    
    def __init__(self):
        self.widget = QtGui.QWidget()
        self.createActions()
        self.createTrayIcon()
        
    def createActions(self):
        self.quitAction = QtGui.QAction("&Quit", self.widget, triggered=QtGui.qApp.quit)
        
    def createTrayIcon(self):
        # Create tray icon menu
        self.trayIconMenu = QtGui.QMenu(self.widget)
        self.trayIconMenu.addAction(self.quitAction)
        # Create tray icon
        self.trayIcon = QtGui.QSystemTrayIcon(QtGui.QIcon("tag.gif"), self.widget)
        self.trayIcon.setContextMenu(self.trayIconMenu)
        
    def show(self):
#        self.widget.show()
        self.trayIcon.show()


def main():
    app = QtGui.QApplication(sys.argv)
    QtGui.QApplication.setQuitOnLastWindowClosed(False)
    
    outbox = OutboxUI()
    outbox.show()
#    w = QtGui.QWidget()
#    trayIcon = SystemTrayIcon(QtGui.QIcon("tag.gif"), w)
#    trayIcon.show()
    
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()

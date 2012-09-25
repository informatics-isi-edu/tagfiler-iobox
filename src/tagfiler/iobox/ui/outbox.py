
import sys
from PyQt4 import QtGui


# I'll probably turn this into a class next to take more control of the action
# handling
def _create_tray_icon(controller):
    # Create tray icon menu
    trayIconMenu = QtGui.QMenu()
    
    trayIconMenu.addAction(QtGui.QAction("&Configure", 
                                trayIconMenu, 
                                triggered=controller.configure))

    trayIconMenu.addAction(QtGui.QAction("&Start", 
                                trayIconMenu, 
                                triggered=controller.start))
    
    trayIconMenu.addSeparator()
    
    trayIconMenu.addAction(QtGui.QAction("&Quit", 
                                trayIconMenu, 
                                triggered=controller.quit))
                                
    # Create tray icon
    trayIcon = QtGui.QSystemTrayIcon(QtGui.QIcon("tag.gif"))
    trayIcon.setContextMenu(trayIconMenu)
    return trayIcon


class OutboxController():
    
    def configure(self):
        print 'time to configure'

    def start(self):
        print 'time to start'
        
    def quit(self):
        print "quitin' time!"
        QtGui.qApp.quit()


def main():
    app = QtGui.QApplication(sys.argv)
    QtGui.QApplication.setQuitOnLastWindowClosed(False)
    
    if QtGui.QSystemTrayIcon.isSystemTrayAvailable():
        print 'system tray available'
    else:
        print 'system tray not available'
        sys.exit(1)
    
    if QtGui.QSystemTrayIcon.supportsMessages():
        print 'system supports balloon messages'
        
    controller = OutboxController()
    trayIcon = _create_tray_icon(controller)
    trayIcon.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()

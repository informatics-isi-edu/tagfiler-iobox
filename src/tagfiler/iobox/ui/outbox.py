# 
# Copyright 2010 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Experimental desktop GUI client for the Tagfiler Outbox.
"""

from tagfiler.iobox import config, dao, models, outbox

from PyQt4 import QtCore, QtGui

import sys
import os
import logging
import time


class OutboxTrayIconView():
    """The view of the Outbox system tray icon.
    """
    
    def __init__(self, icon, parent=None):
        """Initializes a new instance.
        
        The required 'icon' parameter must be a QIcon instance. It is used as 
        the default icon on the system tray.
        
        The optional 'parent' parameter must be a QObject.
        """
        
        assert isinstance(icon, QtGui.QIcon)
        assert parent is None or isinstance(parent, QtCore.QObject)
        
        # Create tray icon
        self._tray_icon = QtGui.QSystemTrayIcon(icon, parent)
        
        # Create context menu and menu actions
        menu = QtGui.QMenu(parent)
        
        self.configure_action = QtGui.QAction("&Configure", menu)
        self.console_action = QtGui.QAction("Consol&e", menu)
        self.start_action = QtGui.QAction("St&art", menu)
        self.stop_action = QtGui.QAction("St&op", menu)
        self.stop_action.setEnabled(False)
        self.quit_action = QtGui.QAction("&Quit", menu)
        
        menu.addAction(self.start_action)
        menu.addAction(self.stop_action)
        menu.addSeparator()
        menu.addAction(self.configure_action)
        menu.addAction(self.console_action)
        menu.addSeparator()
        menu.addAction(self.quit_action)
        
        self._tray_icon.setContextMenu(menu)
        
    def show(self):
        """Show the tray icon."""
        self._tray_icon.show()


class OutboxTrayIconController():
    """The controller of the Outbox system tray icon.
    """
    
    def __init__(self, outbox_manager, outbox_model):
        """Initialize the object.
        
        The 'outbox_manager' is required.
        
        The 'outbox_model' is required.
        """
        assert isinstance(outbox_manager, outbox.Outbox)
        assert isinstance(outbox_model, models.Outbox)
        self.outbox_manager = outbox_manager
        self.outbox_model = outbox_model
        
    def configure(self):
        print 'time to configure'
        self.start_action.setEnabled(True)
        
    def console(self):
        print 'console'

    def start(self):
        print 'time to start'
        self.start_action.setEnabled(False)
        self.stop_action.setEnabled(True)
        self.outbox_manager.start()
        
    def stop(self):
        print 'time to stop'
        self.start_action.setEnabled(True)
        self.stop_action.setEnabled(False)
        self.outbox_manager.terminate()
        while not self.outbox_manager.is_terminated():
            print 'waiting for outbox to terminate'
            time.sleep(1)
            
    def quit(self):
        print "quitin' time!"
        QtGui.qApp.quit()


def main():
    logging.basicConfig(level=logging.DEBUG)

    app = QtGui.QApplication(sys.argv)
    app.setApplicationName('Tagfiler Outbox')
    QtGui.QApplication.setQuitOnLastWindowClosed(False)
    
    if QtGui.QSystemTrayIcon.isSystemTrayAvailable():
        print 'system tray available'
    else:
        print 'system tray not available'
        sys.exit(1)
    
    if QtGui.QSystemTrayIcon.supportsMessages():
        print 'system supports balloon messages'
    
    # Load or create outbox model
    # TODO: Process some commandline arguments
    default_name = 'default'
    default_path = os.path.join(os.path.expanduser('~'), 
                                '.tagfiler', 'outbox.conf')
    (outbox_dao, outbox_model) = config.load_or_create_outbox(default_name, default_path)
    state_dao = outbox_dao.get_state_dao(outbox_model)
    outbox_manager = outbox.Outbox(outbox_model, state_dao)
    
    # Create the tray icon view and controller
    controller = OutboxTrayIconController(outbox_manager, outbox_model)
    trayicon = OutboxTrayIconView(QtGui.QIcon("tag.gif"))
    trayicon.configure_action.triggered.connect(controller.configure)
    trayicon.console_action.triggered.connect(controller.console)
    trayicon.start_action.triggered.connect(controller.start)
    trayicon.stop_action.triggered.connect(controller.stop)
    trayicon.quit_action.triggered.connect(controller.quit)
    controller.start_action = trayicon.start_action
    controller.stop_action = trayicon.stop_action
    trayicon.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()

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

from tagfiler.iobox import config, dao, models, outbox, ui
from tagfiler.iobox.ui.preferences import PreferencesDialog

from PyQt4 import QtCore, QtGui

import sys
import os
import logging
import time


logger = logging.getLogger(__name__)


class ConsoleStream():
    """A stream that appends text to the console window text widget. A bit
    kludgy. And it isn't working well. There is a better way to do this."""
    
    def __init__(self, textedit):
        self.textedit = textedit
        
    def write(self, text):
        self.textedit.append(text)


class ConsoleWindow(QtGui.QWidget):
    """Outbox console messages window."""
    
    def __init__(self):
        super(ConsoleWindow, self).__init__()

        self.setup()
        self.setWindowTitle("Console Messages")
        
    def close(self):
        self.hide()

    def setup(self):
        close_button = QtGui.QPushButton("C&lose")
        close_button.clicked.connect(self.close)

        bottomLayout = QtGui.QHBoxLayout()
        bottomLayout.addWidget(close_button)

        self.text_edit = QtGui.QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setMinimumSize(600, 600)

        mainLayout = QtGui.QVBoxLayout()
        mainLayout.addWidget(self.text_edit)
        mainLayout.addLayout(bottomLayout)

        self.setLayout(mainLayout)


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
        
        self.preferences_action = QtGui.QAction("&Preferences...", menu)
        self.help_action = QtGui.QAction("&Help", menu)
        self.console_action = QtGui.QAction("Consol&e Messages", menu)
        self.start_action = QtGui.QAction("St&art", menu)
        self.stop_action = QtGui.QAction("St&op", menu)
        self.stop_action.setEnabled(False)
        self.quit_action = QtGui.QAction("&Quit Outbox", menu)
        
        menu.addAction(self.start_action)
        menu.addAction(self.stop_action)
        menu.addSeparator()
        menu.addAction(self.preferences_action)
        #menu.addAction(self.help_action)
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
    
    def __init__(self, outbox_dao, outbox_model, console_window):
        """Initialize the object.
        
        The 'outbox_dao' is required.
        
        The 'outbox_model' is required.
        """
        assert isinstance(outbox_dao, dao.OutboxDAO)
        assert isinstance(outbox_model, models.Outbox)
        self.outbox_dao = outbox_dao
        self.outbox_model = outbox_model
        self.console_window = console_window
        self.preferences_dialog = PreferencesDialog(self.outbox_model)
        self.outbox_manager = None
        
    def preferences(self):
        self.preferences_dialog.show()
        
    def console(self):
        self.console_window.show()

    def start(self):
        logger.info("Starting Outbox.")
        self.start_action.setEnabled(False)
        self.stop_action.setEnabled(True)
        self.preferences_action.setEnabled(False)
        self.state_dao = self.outbox_dao.get_state_dao(self.outbox_model)
        self.outbox_manager = outbox.Outbox(self.outbox_model, self.state_dao)
        self.outbox_manager.start()
        
    def stop(self):
        logger.info("Stopping Outbox. This may take a few moments.")
        self.outbox_manager.terminate()
        while not self.outbox_manager.is_terminated():
            time.sleep(1)
        logger.info("Outbox stopped.")
        self.start_action.setEnabled(True)
        self.stop_action.setEnabled(False)
        self.preferences_action.setEnabled(True)
            
    def quit(self):
        if self.outbox_manager and not self.outbox_manager.is_terminated():
            self.stop()
        logger.info('Goodbye')
        QtGui.qApp.quit()


def main():
    app = QtGui.QApplication(sys.argv)
    app.setApplicationName('Tagfiler Outbox')
    QtGui.QApplication.setQuitOnLastWindowClosed(False)
    
    # Setup the kludgy console window and stream
    console_window = ConsoleWindow()
    console_out = ConsoleStream(console_window.text_edit)
    
    # Configure log output to console stream
    logging.basicConfig(stream=console_out, level=logging.DEBUG)
    logger.info('Welcome to Tagfiler Outbox.')
    
    if not QtGui.QSystemTrayIcon.isSystemTrayAvailable():
        print 'Your system does not support the desktop system tray.'
        sys.exit(1)
    
    if not QtGui.QSystemTrayIcon.supportsMessages():
        logger.warning('Your system does not support messages.')
    
    default_name = 'default'
    default_path = os.path.join(os.path.expanduser('~'), 
                                '.tagfiler', 'outbox.conf')
    logger.info('Loading configuration from %s.' % default_path)
    (outbox_dao, outbox_model) = \
        config.load_or_create_outbox(default_name, default_path)
        
    # Create the tray icon view and controller
    tag_filename = os.path.join(os.path.dirname(ui.__file__), 'icons', 'tag.gif')
    trayicon = OutboxTrayIconView(QtGui.QIcon(tag_filename))
    controller = OutboxTrayIconController(outbox_dao, outbox_model, console_window)
    controller.start_action = trayicon.start_action
    controller.stop_action = trayicon.stop_action
    controller.preferences_action = trayicon.preferences_action
    
    QtCore.QObject.connect(trayicon.preferences_action, QtCore.SIGNAL('triggered()'), controller.preferences)
    QtCore.QObject.connect(trayicon.console_action, QtCore.SIGNAL('triggered()'), controller.console)
    QtCore.QObject.connect(trayicon.start_action, QtCore.SIGNAL('triggered()'), controller.start)
    QtCore.QObject.connect(trayicon.stop_action, QtCore.SIGNAL('triggered()'), controller.stop)
    QtCore.QObject.connect(trayicon.quit_action, QtCore.SIGNAL('triggered()'), controller.quit)

    trayicon.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()

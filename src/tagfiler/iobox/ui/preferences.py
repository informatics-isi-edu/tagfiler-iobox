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
A PyQt based Preferences dialog for the Tagfiler Outbox.
"""

from tagfiler.iobox import config, dao, models, outbox

from PyQt4 import QtGui


class PreferencesDialog(QtGui.QDialog):
    """The Preferences dialog."""
    
    def __init__(self, outbox_model, parent=None):
        """Constructor.
        
        Requires 'outbox_model' parameter of type models.Outbox.
        """
        super(PreferencesDialog, self).__init__(parent)
        
        tabWidget = QtGui.QTabWidget()
        tabWidget.addTab(TagfilerTab(outbox_model.get_tagfiler()), "Tagfiler")

        buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel)

        buttonBox.accepted.connect(self.accept)
        buttonBox.rejected.connect(self.reject)

        mainLayout = QtGui.QVBoxLayout()
        mainLayout.addWidget(tabWidget)
        mainLayout.addWidget(buttonBox)
        self.setLayout(mainLayout)

        self.setWindowTitle("Tagfiler Outbox Preferences")


class TagfilerTab(QtGui.QWidget):
    """The Tagfiler tab.
    
    A configuration tab for Tagfiler service details which include the URL to
    the Tagfiler server, and the username and password for the user setting up
    the Outbox.
    """
    
    def __init__(self, tagfiler, parent=None):
        """Constructor.
        
        Requires 'tagfiler' parameter of models.Tagfiler type.
        """
        super(TagfilerTab, self).__init__(parent)

        urlLabel = QtGui.QLabel("URL:")
        urlEdit = QtGui.QLineEdit(tagfiler.get_url())
        usernameLabel = QtGui.QLabel("Username:")
        usernameEdit = QtGui.QLineEdit(tagfiler.get_username())
        passwordLabel = QtGui.QLabel("Password:")
        passwordEdit = QtGui.QLineEdit(tagfiler.get_password())
        passwordEdit.setEchoMode(QtGui.QLineEdit.Password)

        layout = QtGui.QVBoxLayout()
        layout.addWidget(urlLabel)
        layout.addWidget(urlEdit)
        layout.addWidget(usernameLabel)
        layout.addWidget(usernameEdit)
        layout.addWidget(passwordLabel)
        layout.addWidget(passwordEdit)
        layout.addStretch(1)
        self.setLayout(layout)


if __name__ == '__main__':

    import sys, os

    app = QtGui.QApplication(sys.argv)

    if len(sys.argv) >= 2:
        fileName = sys.argv[1]
    else:
        fileName = "."

    default_name = 'default'
    default_path = os.path.join(os.path.expanduser('~'), 
                                '.tagfiler', 'outbox.conf')
    print('Loading configuration from %s.' % default_path)
    (outbox_dao, outbox_model) = \
        config.load_or_create_outbox(default_name, default_path)
        
    dialog = PreferencesDialog(outbox_model)
    sys.exit(dialog.exec_())

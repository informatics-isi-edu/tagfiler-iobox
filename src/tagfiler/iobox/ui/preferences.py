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

from tagfiler.iobox import config

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
        tabWidget.addTab(RootTab(outbox_model.get_roots()), "Root Directories")
        tabWidget.addTab(PatternTab(outbox_model.get_exclusion_patterns()), "Exclusion Patterns")
        tabWidget.addTab(PatternTab(outbox_model.get_inclusion_patterns()), "Inclusion Patterns")
        tabWidget.addTab(RERuleTab(outbox_model.get_path_rules()), "Path Rules")

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


class RootTab(QtGui.QWidget):
    def __init__(self, roots, parent=None):
        """Constructor.
        
        Requires 'roots' parameter. A list of models.Root objects.
        """
        super(RootTab, self).__init__(parent)
        
        rootsLabel = QtGui.QLabel("Roots")
        rootsList = QtGui.QListWidget()
        
        for root in roots:
            rootsList.addItem(root.get_filepath())
        
        layout = QtGui.QVBoxLayout()
        layout.addWidget(rootsLabel)
        layout.addWidget(rootsList)
        self.setLayout(layout)


class PatternTab(QtGui.QWidget):
    def __init__(self, patterns, parent=None):
        """Constructor.
        
        Requires 'patterns' parameter. A list of models.Pattern objects.
        """
        super(PatternTab, self).__init__(parent)
        
        plabel = QtGui.QLabel("Patterns")
        plist = QtGui.QListWidget()
        
        for pattern in patterns:
            list.addItem(pattern.get_pattern())
        
        layout = QtGui.QVBoxLayout()
        layout.addWidget(plabel)
        layout.addWidget(plist)
        self.setLayout(layout)


class RERuleTab(QtGui.QWidget):
    def __init__(self, rerules, parent=None):
        """Constructor.
        
        Requires 'rerules' parameter. A list of models.RERules objects.
        """
        super(RERuleTab, self).__init__(parent)
        
        label = QtGui.QLabel("Rules")
        table = QtGui.QTableWidget(0, 5)
        table.setHorizontalHeaderLabels(("Name", "Pre-Pattern", "Pattern", "Apply", "Extract"))
        table.horizontalHeader().setResizeMode(0, QtGui.QHeaderView.Stretch)
                
        row = 0
        for rule in rerules:
            table.insertRow(row)
            
            name = rule.get_name()
            if name is None:
                name = "None"
                
            prepattern = rule.get_prepattern()
            if prepattern is None:
                prepattern = "None"
                
            table.setItem(row, 0, QtGui.QTableWidgetItem(name))
            table.setItem(row, 1, QtGui.QTableWidgetItem(prepattern))
            table.setItem(row, 2, QtGui.QTableWidgetItem(rule.get_pattern()))
            table.setItem(row, 3, QtGui.QTableWidgetItem(rule.get_apply()))
            table.setItem(row, 4, QtGui.QTableWidgetItem(rule.get_extract()))
            row += 1
        
        layout = QtGui.QVBoxLayout()
        layout.addWidget(label)
        layout.addWidget(table)
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

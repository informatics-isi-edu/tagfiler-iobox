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
Model classes for representing state in the Outbox.
"""


class Outbox(object):
    """Outbox configuration object that retains information about its 
    tagfiler, roots, inclusion/exclusion patterns, path/line matches.
    """
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.state_db = kwargs.get("state_db")
        self.bulk_ops_max = kwargs.get("bulk_ops_max")
        self.endpoint_name = kwargs.get("endpoint_name")
        self.url = kwargs.get("url")
        self.username = kwargs.get("username")
        self.password = kwargs.get("password")
        self.roots = kwargs.get("roots", [])
        self.includes = kwargs.get("includes", [])
        self.excludes = kwargs.get("excludes", [])
        self.path_rules = kwargs.get("path_rules", [])
        self.line_rules = kwargs.get("line_rules", [])


class RERule(object):
    """Regular expression used in an outbox for tagging."""
    def __init__(self, **kwargs):
        self.prepattern = kwargs.get("prepattern")
        self.pattern = kwargs.get("pattern")
        self.apply = kwargs.get("apply", "match")
        self.extract = kwargs.get("extract", "single")
        self.tags = kwargs.get("tags", [])
        self.templates = kwargs.get("templates", [])
        self.rewrites = kwargs.get("rewrites", [])
        self.constants = kwargs.get("constants", [])


class LineRule(object):
    def __init__(self, **kwargs):
        self.path_rule = kwargs.get("path_rule")
        self.rerules = kwargs.get("rerules", [])


class RERuleConstant(object):
    """Constant assigned to a rerule."""
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.value = kwargs.get("value")        


class RERuleRewrite(object):
    """Rewrite patterns and templates for a rerule."""
    def __init__(self, **kwargs):
        self.pattern = kwargs.get("pattern")
        self.template = kwargs.get("template")


class File(object):
    """File statistics that describe a file retrieved during scan or register.
    """
    
    COMPUTE     = 0
    COMPARE     = 1
    REGISTER    = 2
    
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.filename = kwargs.get("filename")
        self.mtime = kwargs.get("mtime")
        self.rtime = kwargs.get("rtime")
        self.size = kwargs.get("size")
        self.checksum = kwargs.get("checksum")
        self.username = kwargs.get("username")
        self.groupname = kwargs.get("groupname")
        self.tags = kwargs.get("tags", [])
        self.status = kwargs.get("status")
        
    def get_tags(self):
        return self.tags
    def set_tags(self, tags):
        self.tags = tags
    def add_tag(self, tag):
        self.tags.append(tag)
    def get_tag(self, tag_name):
        tag = []
        for t in self.tags:
            if t.name == tag_name:
                tag.append(t)
        return tag

    def __str__(self):
        s = self.filename
        s += " <%s> " % self.id
        s += " (%s %s %s %s %s) [" % (self.mtime, self.rtime, self.size, 
                                      self.username, self.groupname)
        for t in self.tags:
            s += "%s, " % t
        s += "]"
        return s


class RegisterTag(object):
    """Tag that is assigned to a registered file."""
    def __init__(self, name=None, value=None, **kwargs):
        self.name = name or kwargs.get("name")
        self.value = value or kwargs.get("value")

    def __str__(self):
        s = "%s=%s" % (self.name, self.value)
        return s


class ScanState(object):
    """Current state of a scan.
    """
    
    def __init__(self, **kwargs):
        self.id = kwargs.get("scan_state_id")
        self.state = kwargs.get("state")
    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def set_state(self, state):
        self.state = state
    def get_state(self):
        return self.state


class Scan(object):
    """File scan that maintains information about its start/end time, current state, and files.

    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.start = kwargs.get("start")
        self.end = kwargs.get("end")
        self.state = ScanState(**kwargs)
        self.files = []

    def set_id(self, i):
        self.id = id
    def get_id(self):
        return self.id
    def set_start(self, start):
        self.start = start
    def get_start(self):
        return self.start
    def set_end(self, end):
        self.end = end
    def get_end(self):
        return self.end
    def set_state(self, state):
        self.state = state
    def get_state(self):
        return self.state
    def get_files(self):
        return self.files
    def set_files(self, files):
        self.files = files
    def add_file(self, f):
        self.files.append(f)


def create_default_name_path_rule(endpoint_name):
    path_rule = RERule()
    path_rule.pattern = '^(?P<path>.*)'
    path_rule.extract = 'template'
    path_rule.templates.append('file://%s\g<path>' % endpoint_name)
    path_rule.tags.append('name')
    return path_rule

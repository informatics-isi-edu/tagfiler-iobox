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
    """outbox configuration object that retains information about its tagfiler, roots, inclusion/exclusion patterns, path/line matches.
    
    """
    def __init__(self, **kwargs):
        self.name = kwargs.get("name")
        self.state_db = kwargs.get("state_db")
        self.bulk_ops_max = kwargs.get("bulk_ops_max")
        self.endpoint_name = kwargs.get("endpoint_name")
        self.tagfiler = Tagfiler(**kwargs)
        self.roots = kwargs.get("roots", [])
        self.includes = kwargs.get("includes", [])
        self.excludes = kwargs.get("excludes", [])
        self.path_rules = []
        self.line_rules = []

    def get_tagfiler(self):
        return self.tagfiler
    def set_tagfiler(self, tagfiler):
        self.tagfiler = tagfiler
    def get_all_rules(self):
        all_rules = []
        all_rules.extend(self.path_rules)
        all_rules.extend(self.line_rules)
        return all_rules

    def get_path_rules(self):
        return self.path_rules
    def set_path_rules(self, path_rules):
        self.path_rules = path_rules
    def add_path_rule(self, path_rule):
        self.path_rules.append(path_rule)
    def get_line_rules(self):
        return self.line_rules
    def set_line_rules(self, line_rules):
        self.line_rules = line_rules
    def add_line_rule(self, line_rule):
        self.line_rules.append(line_rule)

class Tagfiler(object):
    """Tagfiler configuration object that retains information about its url, username, and password.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("tagfiler_id")
        self.url = kwargs.get("tagfiler_url")
        self.username = kwargs.get("tagfiler_username")
        self.password = kwargs.get("tagfiler_password")
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def get_url(self):
        return self.url
    def set_url(self, url):
        self.url = url
    def get_username(self):
        return self.username
    def set_username(self, username):
        self.username = username
    def get_password(self):
        return self.password
    def set_password(self, password):
        self.password = password

class RERule(object):
    """Regular expression used in an outbox for tagging.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.prepattern = None # TODO: populate?
        self.pattern = kwargs.get("pattern")
        self.apply = kwargs.get("apply", "match")
        self.extract = kwargs.get("extract", "single")
        self.rewrites = []
        self.constants = []
        self.tags = []
        self.templates = []

    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def set_name(self, name):
        self.name = name
    def get_name(self):
        return self.name
    def set_prepattern(self, prepattern):
        self.prepattern = prepattern
    def get_prepattern(self):
        return self.prepattern
    def set_pattern(self, pattern):
        self.pattern = pattern
    def get_pattern(self):
        return self.pattern
    def set_apply(self, a):
        self.apply = a
    def get_apply(self):
        return self.apply
    def set_extract(self, extract):
        self.extract = extract
    def get_extract(self):
        return self.extract
    def get_tags(self):
        return self.tags
    def set_tags(self, tags):
        self.tags = tags
    def add_tag(self, tag):
        self.tags.append(tag)
    def get_rewrites(self):
        return self.rewrites
    def set_rewrites(self, rewrites):
        self.rewrites = rewrites
    def add_rewrite(self, rewrite):
        self.rewrites.append(rewrite)
    def get_constants(self):
        return self.constants
    def set_constants(self, constants):
        self.constants = constants
    def add_constant(self, constant):
        self.constants.append(constant)
    def get_templates(self):
        return self.templates
    def set_templates(self, templates):
        self.templates = templates
    def add_template(self, template):
        self.templates.append(template)

class PathRule(RERule):
    pass

class LineRule(object):
    def __init__(self, **kwargs):
        self.path_rule = None
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.rerules = []
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def get_name(self):
        return self.name
    def set_name(self, name):
        self.name = name
    def get_path_rule(self):
        return self.path_rule
    def set_path_rule(self, path_rule):
        self.path_rule = path_rule
    def get_rerules(self):
        return self.rerules
    def set_rerules(self, rerules):
        self.rerules = rerules
    def add_rerule(self, rerule):
        self.rerules.append(rerule)

class RERuleComponent(object):
    """Abstract parent for components associated with a rerule.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.rerule_id = kwargs.get("rerule_id")
        
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def set_rerule_id(self, i):
        self.rerule_id = i
    def get_rerule_id(self):
        return self.rerule_id

class RERuleTag(RERuleComponent):
    """Tag assigned to a rerule.
    
    """
    def __init__(self, **kwargs):
        super(RERuleTag, self).__init__(**kwargs)
        self.tag_name = kwargs.get("tag_name")

    def get_tag_name(self):
        return self.tag_name
    def set_tag_name(self, tag_name):
        self.tag_name = tag_name
    

class RERuleTemplate(RERuleComponent):
    """Template assigned to a rerule.
    
    """
    def __init__(self, **kwargs):
        super(RERuleTemplate, self).__init__(**kwargs)
        self.template = kwargs.get("template")
    def get_template(self):
        return self.template
    def set_template(self, template):
        self.template = template

class RERuleConstant(RERuleComponent):
    """Constant assigned to a rerule.
    
    """
    def __init__(self, **kwargs):
        super(RERuleConstant, self).__init__(**kwargs)
        self.constant_name = kwargs.get("constant_name")
        self.constant_value = kwargs.get("constant_value")
    def set_constant_name(self, constant_name):
        self.constant_name = constant_name
    def get_constant_name(self):
        return self.constant_name
    def set_constant_value(self, constant_value):
        self.constant_value = constant_value
    def get_constant_value(self):
        return self.constant_value

class RERuleRewrite(RERuleComponent):
    """Rewrite patterns and templates for a rerule.
    
    """
    def __init__(self, **kwargs):
        super(RERuleRewrite, self).__init__(**kwargs)
        self.rewrite_pattern = kwargs.get("rewrite_pattern")
        self.rewrite_template = kwargs.get('rewrite_template')
    def get_rewrite_pattern(self):
        return self.rewrite_pattern
    def set_rewrite_pattern(self, rewrite_pattern):
        self.rewrite_pattern = rewrite_pattern
        
    def get_rewrite_template(self):
        return self.rewrite_template
    def set_rewrite_template(self, rewrite_template):
        self.rewrite_template = rewrite_template


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
            if t.get_tag_name() == tag_name:
                tag.append(t)
        return tag

    def __str__(self):
        s = self.filename
        s += " <%s> " % self.id
        s += " (%s %s %s %s %s) [" % (self.mtime, self.rtime, self.size, 
                                      self.username, self.groupname)
        for t in self.tags:
            s += "%s=%s, " % (t.get_tag_name(), t.get_tag_value())
        s += "]"
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

class RegisterTag(object):
    """Tag that is assigned to a registered file.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.tag_name = kwargs.get("tag_name")
        self.tag_value = kwargs.get("tag_value")
        
    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def get_tag_name(self):
        return self.tag_name
    def set_tag_name(self, tag_name):
        self.tag_name = tag_name
    def get_tag_value(self):
        return self.tag_value
    def set_tag_value(self, tag_value):
        self.tag_value = tag_value

'''
class RegisterFile(object):
    """File that should be registered in tagfiler
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("register_file_id")
        self.file = File(**kwargs)
        self.added = kwargs.get("added")
        self.tags = []

    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def get_file(self):
        return self.file
    def set_file(self, f):
        self.file = f
    def get_added(self):
        return self.added
    def set_added(self, a):
        self.added = a
    def get_tags(self):
        return self.tags
    def set_tags(self, tags):
        self.tags = tags
    def add_tag(self, tag):
        self.tags.append(tag)
    def get_tag(self, tag_name):
        tag = []
        for t in self.tags:
            if t.get_tag_name() == tag_name:
                tag.append(t)
        return tag

    def __str__(self):
        s = "%s [" % str(self.file)
        for t in self.tags:
            s += "%s=%s, " % (t.get_tag_name(), t.get_tag_value())
        s += "]"
        return s
'''

def create_default_name_path_rule(endpoint_name):
    path_rule = PathRule()
    path_rule.set_pattern('^(?P<path>.*)')
    path_rule.set_extract('template')
    t1 = RERuleTemplate()
    t1.set_template('file://%s\g<path>' % endpoint_name)
    path_rule.add_template(t1)
    tg1 = RERuleTag()
    tg1.set_tag_name('name')
    path_rule.add_tag(tg1)
    
    return path_rule

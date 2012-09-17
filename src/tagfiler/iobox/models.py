'''
Created on Sep 10, 2012

@author: smithd
'''

class Outbox(object):
    """outbox configuration object that retains information about its tagfiler, roots, inclusion/exclusion patterns, path/line matches.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("outbox_id")
        self.name = kwargs.get("outbox_name")
        self.tagfiler = Tagfiler(**kwargs)
        self.roots = []
        self.inclusion_patterns = []
        self.exclusion_patterns = []
        self.path_matches = []
        self.line_matches = []

    def set_id(self, i):
        self.id = i
    
    def get_id(self):
        return self.id
    def get_name(self):
        return self.name
    def set_name(self, name):
        self.name = name
    
    def get_tagfiler(self):
        return self.tagfiler
    def set_tagfiler(self, tagfiler):
        self.tagfiler = tagfiler
    def get_roots(self):
        return self.roots
    def set_roots(self, roots):
        self.roots = roots
    def add_root(self, r):
        self.roots.append(r)
    def get_inclusion_patterns(self):
        return self.inclusion_patterns
    def set_inclusion_patterns(self, inclusion_patterns):
        self.inclusion_patterns = inclusion_patterns
    def add_inclusion_pattern(self, inclusion_pattern):
        self.inclusion_patterns.append(inclusion_pattern)
    def get_exclusion_patterns(self):
        return self.exclusion_patterns
    def set_exclusion_patterns(self, exclusion_patterns):
        self.exclusion_patterns = exclusion_patterns
    def add_exclusion_pattern(self, exclusion_pattern):
        self.exclusion_patterns.append(exclusion_pattern)
    def get_path_matches(self):
        return self.path_matches
    def set_path_matches(self, path_matches):
        self.path_matches = path_matches
    def add_path_match(self, path_match):
        self.path_matches.append(path_match)
    def get_line_matches(self):
        return self.line_matches
    def set_line_matches(self, line_matches):
        self.line_matches = line_matches
    def add_line_match(self, line_match):
        self.line_matches.append(line_match)

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

class Root(object):
    """File scan root assigned to an outbox.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.filepath = kwargs.get("filepath")
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i

    def get_filepath(self):
        return self.filepath
    def set_filepath(self, filepath):
        self.filepath = filepath

class Pattern(object):
    """Abstract parent for patterns associated with an outbox.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.pattern = kwargs.get("pattern")
    def get_pattern(self):
        return self.pattern
    def set_pattern(self, pattern):
        self.pattern = pattern
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i

class ExclusionPattern(Pattern):
    """Exclusion pattern associated with an outbox.
    
    """
    pass

class InclusionPattern(Pattern):
    """Inclusion pattern associated with an outbox.
    
    """
    pass

class PathMatch(object):
    """Path match rule associated with an outbox.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.outbox_id = kwargs.get("outbox_id")
        self.name = kwargs.get("name")
        self.pattern = kwargs.get("pattern")
        self.extract = kwargs.get("extract")
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
    def set_pattern(self, pattern):
        self.pattern = pattern
    def get_pattern(self):
        return self.pattern
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
    def set_outbox_id(self, i):
        self.outbox_id = i

    def get_templates(self):
        return self.templates
    def set_templates(self, templates):
        self.templates = templates
    def add_template(self, template):
        self.templates.append(template)

class PathMatchComponent(object):
    """Abstract parent for components associated with a path match rule.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.path_match_id = kwargs.get("path_match_id")
        
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def set_path_match_id(self, i):
        self.path_match_id = i

class PathMatchTag(PathMatchComponent):
    """Tag assigned to a path match rule.
    
    """
    def __init__(self, **kwargs):
        super(PathMatchTag, self).__init__(**kwargs)
        self.tag_name = kwargs.get("tag_name")

    def get_tag_name(self):
        return self.tag_name
    def set_tag_name(self, tag_name):
        self.tag_name = tag_name
    

class PathMatchTemplate(PathMatchComponent):
    """Template assigned to a patch match rule.
    
    """
    def __init__(self, **kwargs):
        super(PathMatchTemplate, self).__init__(**kwargs)
        self.template = kwargs.get("template")
    def get_template(self):
        return self.template
    def set_template(self, template):
        self.template = template

class PathRule(object):
    """Path rule associated with a line rule.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("path_rule_id")
        self.pattern = kwargs.get("pattern")

    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def set_pattern(self, pattern):
        self.pattern = pattern
    def get_pattern(self):
        return self.pattern

class LineMatch(object):
    """Line match rule associated with an outbox.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("line_match_id")
        self.outbox_id = kwargs.get("outbox_id")
        self.name = kwargs.get("name")
        self.path_rule = PathRule(**kwargs)
        self.line_rules = []

    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def set_outbox_id(self, i):
        self.outbox_id = i
    def get_outbox_id(self):
        return self.outbox_id
    def set_name(self, name):
        self.name = name
    def get_name(self):
        return self.name
    def get_path_rule(self):
        return self.path_rule
    def set_path_rule(self, path_rule):
        self.path_rule = path_rule
    def get_line_rules(self):
        return self.line_rules
    def set_line_rules(self, line_rules):
        self.line_rules = line_rules
    def add_line_rule(self, line_rule):
        self.line_rules.append(line_rule)

class LineRulePrepattern(object):
    """Prepattern associated with a line rule.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("line_rule_prepattern_id")
        self.pattern = kwargs.get("line_rule_prepattern_pattern")
    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def get_pattern(self):
        return self.pattern
    def set_pattern(self, pattern):
        self.pattern = pattern

class LineRule(object):
    """Tagging rule associated with a line rule.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.pattern = kwargs.get("pattern")
        self.apply = kwargs.get("apply")
        self.extract = kwargs.get("extract")
        self.line_match_id = kwargs.get("line_match_id")
        self.prepattern = LineRulePrepattern(**kwargs)

    def set_id(self, i):
        self.id = i
    def get_id(self):
        return self.id
    def set_pattern(self, pattern):
        self.pattern = pattern
    def get_pattern(self):
        return self.pattern
    def set_apply(self, ap):
        self.apply = ap
    def get_apply(self):
        return self.apply
    def set_extract(self, extract):
        self.extract = extract
    def get_extract(self):
        return self.extract
    def get_prepattern(self):
        return self.prepattern
    def set_prepattern(self, prepattern):
        self.prepattern = prepattern
    def set_line_match_id(self, line_match_id):
        self.line_match_id = line_match_id
    def get_line_match_id(self):
        return self.line_match_id

# Instance classes
class File(object):
    """File statistics that describe a file retrieved during scan or register.
    
    """
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.filepath = kwargs.get("filepath")
        self.mtime = kwargs.get("mtime")
        self.size = kwargs.get("size")
        self.checksum = kwargs.get("checksum")
        self.must_tag = kwargs.get("must_tag", True)
    def get_id(self):
        return self.id
    def set_id(self, i):
        self.id = i
    def set_filepath(self, filepath):
        self.filepath = filepath
    def get_filepath(self):
        return self.filepath
    def set_mtime(self, mtime):
        self.mtime = mtime
    def get_mtime(self):
        return self.mtime
    def set_size(self, size):
        self.size = size
    def get_size(self):
        return self.size
    def set_checksum(self, checksum):
        self.checksum = checksum
    def get_checksum(self):
        return self.checksum
    def set_must_tag(self, must_tag):
        self.must_tag = must_tag
    def get_must_tag(self):
        return self.must_tag

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
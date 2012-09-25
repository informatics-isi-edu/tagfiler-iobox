'''
Created on Sep 19, 2012

@author: smithd
'''
from tagfiler.iobox.models import PathRule, LineRule, RegisterTag
import re

class RERuleProcessor(object):
    """Processes a rerule object into tags.
    
    """
    def __init__(self, rerule):
        """Constructor
        
        Keyword arguments:
        rerule -- rerule object
        
        """
        self._rerule = rerule
        self.prepattern_processor = None
        if self._rerule.get_prepattern() is not None:
            self.prepattern_processor = RERuleProcessor(self.get_prepattern())
        self.pattern = re.compile(rerule.get_pattern())

        self.apply_func = dict(match=self.apply_match,
                          search=self.apply_search,
                          finditer=self.apply_finditer)[rerule.get_apply()]

        self.tester_func = dict(match=re.match,
                           search=re.search,
                           finditer=re.search)[rerule.get_apply()]

        self.extract_func = dict(constants=self.extract_constant,
                            single=self.extract_single, 
                            positional=self.extract_positional,
                            named=self.extract_named,
                            template=self.extract_template)[rerule.get_extract()]

        self.rewrites = [ (re.compile(r.get_rewrite_pattern()), r.get_rewrite_template()) for r in rerule.get_rewrites() ]

        self.constants = rerule.get_constants()

        self.tags = rerule.get_tags()

        self.templates = rerule.get_templates()
        
    def test(self, string):
        if self.prepattern and not self.prepattern.test(string):
            return False
        if self.tester_func(self.pattern, string):
            return True
        else:
            return False

    def analyze(self, string):
        if self.prepattern_processor and not self.prepattern_processor.test(string):
            return dict()
        return self.apply_func(string)

    def rewrite(self, valuestring):
        for pattern, template in self.rewrites:
            valuestring = re.sub(pattern, template, valuestring)
        return valuestring

    def extract_constant(self, match):
        if match:
            return self.constants
        else:
            return dict()

    def extract_single(self, match):
        if match:
            return { self.tags[0].get_tag_name() : set([ self.rewrite(match.group(0)) ]) }
        else:
            return dict()

    def extract_positional(self, match):
        if match:
            return dict([ (self.tags[i].get_tag_name(), set( [self.rewrite(match.group(i+1))] )) for i in range(0, len(self.tags))
                          if self.tags[i] and self.rewrite(match.group(i+1)) ])
        else:
            return dict()

    def extract_named(self, match):
        if match:
            return dict([ (key, set([ self.rewrite(value) ]) ) for key, value in match.groupdict().items() if self.rewrite(value) ])
        else:
            return dict()

    def extract_template(self, match):
        if match:
            return dict([ (self.tags[i].get_tag_name(), set([self.rewrite(match.expand(self.templates[i].get_template()))]) ) for i in range(0, len(self.tags)) ])
        else:
            return dict()

    def apply_match(self, string):
        return self.extract_func( re.match(self.pattern, string) )

    def apply_search(self, string):
        return self.extract_func( re.search(self.pattern, string) )

    def apply_finditer(self, string):
        def dictmerge(tags, newtags):
            for tag, valset in newtags.items():
                if type(valset) != set:
                    if type(valset) == list:
                        valset = set(valset)
                    else:
                        valset = set([valset])
                if not tags.has_key(tag):
                    tags[tag] = valset.copy()
                else:
                    tags[tag].update(valset)

        tags = dict()
        for match in re.finditer(self.pattern, string):
            dictmerge(tags, self.extract_func(match))
        return tags

class PathRuleProcessor(RERuleProcessor):
    def analyze(self, file_path):
        return super(PathRuleProcessor, self).analyze(file_path)

class TagDirector(object):
    def tag_registered_file(self, rules, registered_file):
        for rule in rules:
            tag_dict = self.get_rule_processor(rule).analyze(registered_file.get_file().get_filepath())
            for k,v_list in tag_dict.iteritems():
                for v in v_list:
                    t = RegisterTag()
                    t.set_tag_name(k)
                    t.set_tag_value(v)
                    registered_file.add_tag(t)

    def get_rule_processor(self, rule):
        if isinstance(rule, PathRule):
            return PathRuleProcessor(rule)
        else:
            raise TypeError("Unsupported rule type for %s" % unicode(rule))


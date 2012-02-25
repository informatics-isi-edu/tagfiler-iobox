
import re

def dictmerge(tags, newtags):
    for tag, valset in newtags.items():
        if not tags.has_key(tag):
            tags[tag] = valset.copy()
        else:
            tags[tag].update(valset)

class RERule:
    """Rule for RE patterns.

       Uses Python RE module to test string via pattern match and
       generate tag-value dictionary when match is True.
    """
    dummy_rule = { 'prepattern' : None,
                   'pattern' : '.*',
                   'apply' : 'match',
                   'extract' : 'single',
                   'rewrites' : [],
                   'constants' : {},
                   'tags': ['tag name'],
                   'templates': [] }

    def __init__(self, rule):
        """Construct RERule.

        Methods:
           rule.analyze(string) -> { tagname : set(values...) }
           rule.test(string) -> boolean

        Rule is a Python dictionary with named parameters:

          prepattern: a recursive use of RERule rule structure, defining a 

          pattern: a Python RE pattern string suitable for re.compile(pattern)

          contstants: { tagname : set(values...), ... }
            -- only if extract=constants

          tags: [ tagname... ]
            -- only if extract=single or extract=positional

          templates: [ template... ]
            -- templates[i] corresponds to tags[i]
            -- only if extract=template

          apply: an application keyword
             'match': use re.match(pattern, rfpath) so find prefix on rfpath
             'search': use re.search(pattern, rfpath) so find substring in rfpath
             'finditer': use re.finditer(pattern, rfpath) so find multiple substrings in rfpath

          extract: an extract method keyword for how to process each match object
             'constants': consult rule['constants'] for tag-val set when match is True
             'single': use match.group(0) as value with rule['tags'][0] as tag name
             'positional': use match.group(i+1) as value with rule['tags'][i] as tag name
             'named': use match.groupdict() directly as tag-val set
             'template': use match.expand(template) for each tags[i], templates[i] pair

        """
        if rule.has_key('prepattern'):
            self.prepattern = RERule(rule['prepattern'])
        else:
            self.prepattern = None

        self.pattern = re.compile(rule['pattern'])

        self.apply = dict(match=self.apply_match,
                          search=self.apply_search,
                          finditer=self.apply_finditer)[rule.get('apply', 'match')]

        self.tester = dict(match=re.match,
                           search=re.search,
                           finditer=re.search)[rule.get('apply', 'match')]

        self.extract = dict(constants=self.extract_constant,
                            single=self.extract_single, 
                            positional=self.extract_positional,
                            named=self.extract_named,
                            template=self.extract_template)[rule.get('extract','single')]

        self.rewrites = [ (re.compile(pattern), template) for pattern, template in rule.get('rewrites', []) ]

        self.constants = rule.get('constants', {})

        self.tags = rule.get('tags', [])

        self.templates = rule.get('templates', [])
        
    def test(self, string):
        if self.prepattern and not self.prepattern.test(string):
            return False
        if self.tester(self.pattern, string):
            return True
        else:
            return False

    def analyze(self, string):
        if self.prepattern and not self.prepattern.test(string):
            return dict()
        return self.apply(string)

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
            return { self.tags[0] : set([ self.rewrite(match.group(0)) ]) }
        else:
            return dict()

    def extract_positional(self, match):
        if match:
            return dict([ (self.tags[i], set( [self.rewrite(match.group(i+1))] )) for i in range(0, len(self.tags))
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
            return dict([ (self.tags[i], self.rewrite(match.expand(self.templates[i]))) for i in range(0, len(self.tags)) ])
        else:
            return dict()

    def apply_match(self, string):
        return self.extract( re.match(self.pattern, string) )

    def apply_search(self, string):
        return self.extract( re.search(self.pattern, string) )

    def apply_finditer(self, string):
        tags = dict()
        for match in re.finditer(self.pattern, string):
            dictmerge(tags, self.extract(match))
        return tags


class REPathRule:
    """RE rule for tagging based on rfpath strings.

       Uses RERule language applied to rfpath value, ignoring physical file.

       See documentation for RERule class for rule structure.
    """

    rulename = 'pathmatch'

    def __init__(self, rule):
        self.rerule = RERule(rule)

    def analyze(self, rfpath, fpath):
        return self.rerule.analyze(rfpath)

class RELineRule:
    """RE rules for tagging based on lines from file.

       Uses RERule language applied to file content at fpath.

       
    """

    rulename = 'linematch'

    def __init__(self, rule):
        """Construct RELineRule .

           rule = {
              pathmatch = RERule,
              linerules = [ RERule... ],
           }

           pathmatch.test(rfpath) must be true in order to apply linerules (default matches every rfpath)
           linerules derive tag values from lines of file input
        """
        self.pathmatch = RERule(rule.get('pathrule', { 'pattern' : '.*' }))
        self.linerules = [ RERule(r) for r in rule.get('linerules', []) ]

    def analyze(self, rfpath, fpath):
        tags = dict()
        if self.pathmatch.test(rfpath):
            f = open(fpath, 'r')
            try:
                line = f.readline()
                while line:
                    for rerule in self.linerules:
                        dictmerge(tags, rerule.analyze(line.strip()))
                    line = f.readline()

                return tags
            finally:
                f.close()
        else:
            return tags

ruleclasses = dict([ (c.rulename, c) for c in [ REPathRule, RELineRule ] ])


def rule(rulename, rule):
    """Construct a rule object from a rulename and rule string."""
    return ruleclasses[rulename](rule)


test1 = rule('linematch',
             { 'pathrule' : { 'pattern' : '.*\.csv' },
               'linerules' : [ { 'tags': ['Cancer Model'],
                                 'prepattern' : { 'pattern' : 'Cell_Type_Transferred' },
                                 'pattern' : ',([^,]+)',
                                 'apply' : 'finditer',
                                 'extract' : 'positional',
                                 'rewrites' : [ ('[Aa][Rr][Ff]', 'ARF'),
                                                ('[Ll][Uu][Cc]', 'LUC'),
                                                ('[Pp]53', 'P53') ]
                                 },
                               { 'tags': ['Drug'],
                                 'prepattern' : { 'pattern' : 'Treatment_Type' },
                                 'pattern' : ',([^,]+)',
                                 'apply' : 'finditer',
                                 'extract' : 'positional'
                                 }
                               ]
               }
             )

test2 = test1.analyze('/psoc-labaer.csv', '/scratch/psoc-labaer.csv')


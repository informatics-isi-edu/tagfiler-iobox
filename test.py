
import rules

psoc_b_rules = [ rules.rule('linematch',
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
                            ),
                 rules.rule('linematch',
                            { 'pathrule' : { 'pattern' : '.*\.csv' },
                              'linerules' : [ { 'tags': ['Cancer Model'],
                                                'prepattern' : { 'pattern' : 'Cell type/cell line' },
                                                'pattern' : ',(.+)',
                                                'extract' : 'positional',
                                                'rewrites' : [ ('[Aa][Rr][Ff]', 'ARF'),
                                                               ('[Ll][Uu][Cc]', 'LUC'),
                                                               ('[Pp]53', 'P53') ]
                                                },
                                              { 'tags': ['Drug'],
                                                'prepattern' : { 'pattern' : 'Treatment type' },
                                                'pattern' : ',([^,]+)',
                                                'extract' : 'positional',
                                          'rewrites' : [ ('D', 'd') ]
                                                }
                                              ]
                              }
                            ),
                 rules.rule('pathmatch',
                            { 'pattern' : '/(.*)',
                              'extract' : 'positional',
                              'tags' : [ 'name' ] }),
                 rules.rule('pathmatch',
                            { 'pattern' : '/[Rr][Pp]([1-4])/',
                              'extract' : 'template',
                              'tags' : [ 'Research Project' ],
                              'templates' : [ 'RP\\1' ] }),
                 rules.rule('pathmatch',
                            { 'pattern' : '(Mallick|Nolan|Labaer|Wang)',
                              'apply' : 'search',
                              'extract' : 'positional',
                              'tags' : [ 'Experimentalist' ] }),
                 rules.rule('pathmatch',
                            { 'pattern' : '([Aa][Rr][Ff]|[Pp]53)',
                              'apply' : 'search',
                              'extract' : 'positional',
                              'tags' : [ 'Cancer Model' ] }),
                 rules.rule('pathmatch',
                            { 'pattern' : '/Rp2/WA016L6',
                              'extract' : 'constants',
                              'constants' : { 'Cancer Model' : 'P53' } }),
                 rules.rule('pathmatch',
                            { 'pattern' : '/Rp2/WA016L7',
                              'extract' : 'constants',
                              'constants' : { 'Cancer Model' : 'ARF' } })
                 ]

test2 = psoc_b_rules[0].analyze('/RP4/Labaer/psoc-labaer.csv', '/scratch/RP4/Labaer/psoc-labaer.csv')

test3 = rules.apply_rules(psoc_b_rules, '/scratch', '/RP4/Labaer/psoc-labaer.csv')

test4 = rules.apply_rules(psoc_b_rules, '/scratch', '/RP4/Wang/psoc-wang-3-78.csv')


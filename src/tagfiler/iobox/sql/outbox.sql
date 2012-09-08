-- outbox configuration
CREATE TABLE IF NOT EXISTS tagfiler (
    id INTEGER NOT NULL PRIMARY KEY,
    url TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL
);

-- main table for user scope
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER NOT NULL PRIMARY KEY,
    tagfiler_id INTEGER NOT NULL REFERENCES tagfiler(id),
    name TEXT
);

CREATE TABLE IF NOT EXISTS inclusion_pattern (
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    pattern TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exclusion_pattern (
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    pattern TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS root (
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    filename TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS path_match (
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    name TEXT,
    pattern TEXT NOT NULL,
    extract TEXT
);

CREATE TABLE IF NOT EXISTS path_match_tag(
    id INTEGER NOT NULL PRIMARY KEY,
    pathmatch_id INTEGER NOT NULL REFERENCES path_match(id),
    tag_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS path_match_template(
    id INTEGER NOT NULL PRIMARY KEY,
    pathmatch_id INTEGER NOT NULL REFERENCES path_match(id),
    template TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS path_rule(
    id INTEGER NOT NULL PRIMARY KEY,
    pattern TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS line_match(
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    name TEXT,
    path_rule_id INTEGER NOT NULL REFERENCES path_rule(id)
);

CREATE TABLE IF NOT EXISTS line_rule_prepattern(
    id INTEGER NOT NULL PRIMARY KEY,
    pattern TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS line_rule(
    id INTEGER NOT NULL PRIMARY KEY,
    line_rule_prepattern_id INTEGER REFERENCES line_rule_prepattern(id),
    pattern TEXT,
    apply TEXT,
    extract TEXT,
    line_match_id INTEGER NOT NULL REFERENCES line_match(id)
);

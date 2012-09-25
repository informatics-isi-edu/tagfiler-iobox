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
    name TEXT,
    endpoint_name TEXT
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
    filepath TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rerule (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT,
    prepattern_id INTEGER REFERENCES rerule(id),
    pattern TEXT NOT NULL,
    extract TEXT,
    apply TEXT
);

CREATE TABLE IF NOT EXISTS rerule_tag(
    id INTEGER NOT NULL PRIMARY KEY,
    rerule_id INTEGER NOT NULL REFERENCES rerule(id),
    tag_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rerule_template(
    id INTEGER NOT NULL PRIMARY KEY,
    rerule_id INTEGER NOT NULL REFERENCES rerule(id),
    template TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rerule_rewrite(
    id INTEGER NOT NULL PRIMARY KEY,
    rerule_id INTEGER NOT NULL REFERENCES rerule(id),
    rewrite_pattern TEXT NOT NULL,
    rewrite_template TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rerule_constant(
    id INTEGER NOT NULL PRIMARY KEY,
    rerule_id INTEGER NOT NULL REFERENCES rerule(id),
    constant_name TEXT NOT NULL,
    constant_value TEXT,
    UNIQUE(rerule_id, constant_name)
);

CREATE TABLE IF NOT EXISTS path_rule(
    rerule_id INTEGER NOT NULL PRIMARY KEY REFERENCES rerule(id)
);

CREATE TABLE IF NOT EXISTS line_rule(
    id INTEGER NOT NULL PRIMARY KEY,
    path_rule_id INTEGER REFERENCES path_rule(id),
    name TEXT
);

CREATE TABLE IF NOT EXISTS line_rule_rerule(
    id INTEGER NOT NULL PRIMARY KEY,
    line_rule_id INTEGER NOT NULL REFERENCES line_rule(id),
    rerule_id INTEGER NOT NULL REFERENCES rerule(id)
);

CREATE TABLE IF NOT EXISTS outbox_path_rule(
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    path_rule_id INTEGER NOT NULL REFERENCES path_rule(id),
    UNIQUE(outbox_id, path_rule_id)
);

CREATE TABLE IF NOT EXISTS outbox_line_rule(
    id INTEGER NOT NULL PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    line_rule_id INTEGER NOT NULL REFERENCES line_rule(id),
    UNIQUE(outbox_id, line_rule_id)
);

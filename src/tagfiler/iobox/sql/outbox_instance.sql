CREATE TABLE IF NOT EXISTS scan_state (
    id INTEGER NOT NULL PRIMARY KEY,
    state TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS scan (
    id INTEGER NOT NULL PRIMARY KEY,
    start TIMESTAMP NOT NULL,
    'end' TIMESTAMP,
    scan_state_id INTEGER REFERENCES scan_state(id)
);

CREATE TABLE IF NOT EXISTS file (
    id INTEGER NOT NULL PRIMARY KEY,
    filename TEXT NOT NULL UNIQUE,
    mtime FLOAT8,
    size INTEGER,
    checksum TEXT,
    must_tag BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS scan_files (
    scan_id INTEGER NOT NULL REFERENCES scan(id),
    file_id INTEGER NOT NULL REFERENCES file(id),
    UNIQUE(scan_id, file_id)
);

CREATE TABLE IF NOT EXISTS register_file (
    id INTEGER NOT NULL PRIMARY KEY,
    file_id INTEGER NOT NULL UNIQUE REFERENCES file(id),
    added TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS register_tag (
    id INTEGER NOT NULL PRIMARY KEY,
    register_file_id INTEGER NOT NULL REFERENCES register_file(id),
    tag_name TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    UNIQUE(register_file_id, tag_name, tag_value)
);

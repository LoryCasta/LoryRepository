CREATE TABLE customers (
    
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    notification_label TEXT NOT NULL UNIQUE
);
CREATE TABLE notifications (

    id INTEGER PRIMARY KEY AUTOINCREMENT,
    body TEXT NOT NULL,
    id_customer INTEGER,
    FOREIGN KEY (id_customer) REFERENCES customers(id)
);
CREATE TABLE notification_counters (

    id_customer INTEGER NOT NULL,
    num INTEGER NOT NULL DEFAULT 0,
    day DATE NOT NULL,
    PRIMARY KEY (id_customer, day),
    FOREIGN KEY (id_customer) REFERENCES customers(id)
);
INSERT INTO customers VALUES
    (1, 'Yvonne Nash', 'Los Angeles'),
    (2, 'Justin Wright', 'Jeddah'),
    (3, 'Thomas Hamilton', 'Bangkok'),
    (4, 'Lily Lee', 'Casablanca'),
    (5, 'Angela Davies', 'Addis Ababa'),
    (6, 'Dan Skinner', 'Lahore'),
    (7, 'Dylan Butler', 'Kinshasa'),
    (8, 'Carl Reid', 'Dhaka'),
    (9, 'Jasmine Rampling', 'Karachi'),
    (10, 'Amelia Ross', 'Abidjan')
;
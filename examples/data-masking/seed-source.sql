-- =============================================================
-- Coffee Shop Loyalty Program — Source Database Seed
-- =============================================================
-- A realistic loyalty program database with PII that needs
-- anonymization before sharing with analytics or dev teams.
-- =============================================================

-- ============================================================
-- Schema
-- ============================================================
CREATE SCHEMA IF NOT EXISTS loyalty;

-- ============================================================
-- Enums
-- ============================================================
CREATE TYPE loyalty.membership_tier AS ENUM ('bronze', 'silver', 'gold', 'platinum');

-- ============================================================
-- Tables
-- ============================================================

-- Core customer table — contains PII
CREATE TABLE loyalty.customers (
    id          integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name  text NOT NULL,
    last_name   text NOT NULL,
    email       text NOT NULL UNIQUE,
    phone       text,
    address     text,
    city        text,
    zip_code    text,
    ssn         text,                          -- for identity verification (very sensitive)
    tier        loyalty.membership_tier NOT NULL DEFAULT 'bronze',
    joined_at   timestamp with time zone NOT NULL DEFAULT now()
);

-- Store locations
CREATE TABLE loyalty.stores (
    id      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name    text NOT NULL,
    city    text NOT NULL,
    region  text NOT NULL
);

-- Every coffee purchase — the count per person is sensitive
-- (could identify individuals by purchase frequency)
CREATE TABLE loyalty.purchases (
    id              integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     integer NOT NULL REFERENCES loyalty.customers(id),
    store_id        integer NOT NULL REFERENCES loyalty.stores(id),
    item_name       text NOT NULL,
    item_count      integer NOT NULL DEFAULT 1,
    amount          numeric(10, 2) NOT NULL,
    points_earned   integer NOT NULL DEFAULT 0,
    purchased_at    timestamp with time zone NOT NULL DEFAULT now()
);

-- Points balance — total is sensitive (identifies high-frequency buyers)
CREATE TABLE loyalty.points_balance (
    customer_id     integer PRIMARY KEY REFERENCES loyalty.customers(id),
    total_points    integer NOT NULL DEFAULT 0,
    lifetime_points integer NOT NULL DEFAULT 0,
    last_earned_at  timestamp with time zone
);

-- Reward redemptions
CREATE TABLE loyalty.redemptions (
    id              integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     integer NOT NULL REFERENCES loyalty.customers(id),
    reward_name     text NOT NULL,
    points_spent    integer NOT NULL,
    redeemed_at     timestamp with time zone NOT NULL DEFAULT now()
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX idx_purchases_customer ON loyalty.purchases(customer_id);
CREATE INDEX idx_purchases_store ON loyalty.purchases(store_id);
CREATE INDEX idx_purchases_date ON loyalty.purchases(purchased_at);
CREATE INDEX idx_redemptions_customer ON loyalty.redemptions(customer_id);

-- ============================================================
-- Views
-- ============================================================

-- Customer purchase summary — used by analytics
CREATE VIEW loyalty.customer_summary AS
SELECT
    c.id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.tier,
    pb.total_points,
    pb.lifetime_points,
    count(p.id) AS total_purchases,
    coalesce(sum(p.amount), 0) AS total_spent
FROM loyalty.customers c
LEFT JOIN loyalty.points_balance pb ON pb.customer_id = c.id
LEFT JOIN loyalty.purchases p ON p.customer_id = c.id
GROUP BY c.id, c.first_name, c.last_name, c.email, c.tier,
         pb.total_points, pb.lifetime_points;

-- ============================================================
-- Sample Data
-- ============================================================

-- Stores
INSERT INTO loyalty.stores (name, city, region) VALUES
    ('Downtown Roasters',     'Portland',   'Northwest'),
    ('Pearl District Brew',   'Portland',   'Northwest'),
    ('Capitol Hill Coffee',   'Seattle',    'Northwest'),
    ('Mission Street Beans',  'San Francisco', 'West'),
    ('Brooklyn Grind',        'New York',   'East');

-- Customers (with realistic PII)
INSERT INTO loyalty.customers (first_name, last_name, email, phone, address, city, zip_code, ssn, tier) VALUES
    ('Alice',   'Johnson',  'alice.johnson@gmail.com',    '503-555-0101', '742 Evergreen Terrace',  'Portland',       '97201', '539-48-0120', 'platinum'),
    ('Bob',     'Martinez', 'bob.martinez@yahoo.com',     '206-555-0202', '1600 Pennsylvania Ave',  'Seattle',        '98101', '461-73-9285', 'gold'),
    ('Carol',   'Chen',     'carol.chen@outlook.com',     '415-555-0303', '221B Baker Street',      'San Francisco',  '94102', '182-56-7834', 'silver'),
    ('David',   'Williams', 'david.w@protonmail.com',     '212-555-0404', '350 Fifth Avenue',       'New York',       '10001', '725-14-3690', 'gold'),
    ('Emma',    'Brown',    'emma.brown@icloud.com',      '503-555-0505', '90 Bedford Street',      'Portland',       '97205', '318-62-4057', 'bronze'),
    ('Frank',   'Garcia',   'frank.garcia@gmail.com',     '206-555-0606', '12 Grimmauld Place',     'Seattle',        '98103', '647-29-8153', 'silver'),
    ('Grace',   'Kim',      'grace.kim@hotmail.com',      '415-555-0707', '4 Privet Drive',         'San Francisco',  '94110', '853-41-2976', 'platinum'),
    ('Henry',   'Davis',    'henry.davis@gmail.com',      '212-555-0808', '124 Conch Street',       'New York',       '10013', '294-85-1637', 'bronze'),
    ('Iris',    'Wilson',   'iris.wilson@yahoo.com',      '503-555-0909', '31 Spooner Street',      'Portland',       '97209', '576-38-4021', 'gold'),
    ('Jack',    'Lee',      'jack.lee@outlook.com',       '206-555-1010', '1725 Slough Avenue',     'Seattle',        '98105', '413-67-5928', 'silver');

-- Purchases (varying frequency — Alice and Grace are heavy buyers)
INSERT INTO loyalty.purchases (customer_id, store_id, item_name, item_count, amount, points_earned, purchased_at) VALUES
    -- Alice: 8 purchases (identifiable as a very frequent buyer)
    (1, 1, 'Latte',           1,  5.50, 55,  '2026-01-05 08:00:00+00'),
    (1, 1, 'Cappuccino',      1,  5.00, 50,  '2026-01-08 07:45:00+00'),
    (1, 2, 'Espresso',        2, 7.00,  70,  '2026-01-12 09:00:00+00'),
    (1, 1, 'Mocha',           1,  6.00, 60,  '2026-01-15 08:30:00+00'),
    (1, 1, 'Cold Brew',       1,  5.50, 55,  '2026-01-20 10:00:00+00'),
    (1, 2, 'Latte',           1,  5.50, 55,  '2026-01-25 07:30:00+00'),
    (1, 1, 'Flat White',      1,  5.75, 58,  '2026-02-01 08:15:00+00'),
    (1, 1, 'Americano',       1,  4.50, 45,  '2026-02-05 09:00:00+00'),
    -- Bob: 4 purchases
    (2, 3, 'Drip Coffee',     1,  3.50, 35,  '2026-01-10 07:00:00+00'),
    (2, 3, 'Latte',           1,  5.50, 55,  '2026-01-18 08:00:00+00'),
    (2, 3, 'Cold Brew',       1,  5.50, 55,  '2026-02-02 10:30:00+00'),
    (2, 3, 'Espresso',        1,  3.50, 35,  '2026-02-15 07:15:00+00'),
    -- Carol: 2 purchases
    (3, 4, 'Matcha Latte',    1,  6.50, 65,  '2026-01-20 11:00:00+00'),
    (3, 4, 'Chai Latte',      1,  5.75, 58,  '2026-02-10 10:00:00+00'),
    -- David: 3 purchases
    (4, 5, 'Espresso',        2, 7.00,  70,  '2026-01-07 06:30:00+00'),
    (4, 5, 'Americano',       1,  4.50, 45,  '2026-01-22 07:00:00+00'),
    (4, 5, 'Cold Brew',       1,  5.50, 55,  '2026-02-08 08:45:00+00'),
    -- Emma: 1 purchase
    (5, 1, 'Hot Chocolate',   1,  4.75, 48,  '2026-02-14 15:00:00+00'),
    -- Frank: 3 purchases
    (6, 3, 'Drip Coffee',     1,  3.50, 35,  '2026-01-15 06:45:00+00'),
    (6, 3, 'Drip Coffee',     1,  3.50, 35,  '2026-01-30 06:50:00+00'),
    (6, 3, 'Latte',           1,  5.50, 55,  '2026-02-12 07:30:00+00'),
    -- Grace: 7 purchases (another heavy buyer)
    (7, 4, 'Pour Over',       1,  6.00, 60,  '2026-01-03 08:00:00+00'),
    (7, 4, 'Latte',           1,  5.50, 55,  '2026-01-06 09:15:00+00'),
    (7, 4, 'Mocha',           1,  6.00, 60,  '2026-01-10 08:30:00+00'),
    (7, 4, 'Cortado',         1,  4.50, 45,  '2026-01-14 07:45:00+00'),
    (7, 4, 'Flat White',      1,  5.75, 58,  '2026-01-19 08:00:00+00'),
    (7, 4, 'Espresso',        2, 7.00,  70,  '2026-01-25 09:00:00+00'),
    (7, 4, 'Cold Brew',       1,  5.50, 55,  '2026-02-01 10:00:00+00'),
    -- Henry: 1 purchase
    (8, 5, 'Drip Coffee',     1,  3.50, 35,  '2026-02-20 07:00:00+00'),
    -- Iris: 5 purchases
    (9, 1, 'Latte',           1,  5.50, 55,  '2026-01-08 08:30:00+00'),
    (9, 2, 'Cappuccino',      1,  5.00, 50,  '2026-01-16 09:00:00+00'),
    (9, 1, 'Mocha',           1,  6.00, 60,  '2026-01-24 08:15:00+00'),
    (9, 2, 'Flat White',      1,  5.75, 58,  '2026-02-05 07:45:00+00'),
    (9, 1, 'Cold Brew',       1,  5.50, 55,  '2026-02-15 10:30:00+00'),
    -- Jack: 2 purchases
    (10, 3, 'Americano',      1,  4.50, 45,  '2026-01-20 06:30:00+00'),
    (10, 3, 'Latte',          1,  5.50, 55,  '2026-02-10 07:00:00+00');

-- Points balances (derived from purchases above)
INSERT INTO loyalty.points_balance (customer_id, total_points, lifetime_points, last_earned_at) VALUES
    (1,  448, 448, '2026-02-05 09:00:00+00'),  -- Alice: heavy buyer
    (2,  180, 180, '2026-02-15 07:15:00+00'),
    (3,  123, 123, '2026-02-10 10:00:00+00'),
    (4,  170, 170, '2026-02-08 08:45:00+00'),
    (5,   48,  48, '2026-02-14 15:00:00+00'),
    (6,  125, 125, '2026-02-12 07:30:00+00'),
    (7,  403, 403, '2026-02-01 10:00:00+00'),  -- Grace: heavy buyer
    (8,   35,  35, '2026-02-20 07:00:00+00'),
    (9,  278, 278, '2026-02-15 10:30:00+00'),
    (10, 100, 100, '2026-02-10 07:00:00+00');

-- Redemptions
INSERT INTO loyalty.redemptions (customer_id, reward_name, points_spent, redeemed_at) VALUES
    (1, 'Free Latte',          100, '2026-01-20 10:05:00+00'),
    (1, 'Free Pastry',          75, '2026-02-01 08:20:00+00'),
    (7, 'Free Pour Over',      100, '2026-01-19 08:05:00+00'),
    (9, 'Free Cappuccino',     100, '2026-02-05 07:50:00+00'),
    (2, 'Free Drip Coffee',     50, '2026-02-02 10:35:00+00');

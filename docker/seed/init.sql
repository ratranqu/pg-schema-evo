-- pg-schema-evo: Seed data for integration tests
-- This script populates the source database with various object types.

-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================
-- Enums
-- ============================================================
CREATE TYPE public.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');
CREATE TYPE public.user_role AS ENUM ('admin', 'editor', 'viewer');

-- ============================================================
-- Tables
-- ============================================================
CREATE TABLE public.users (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username text NOT NULL UNIQUE,
    email text NOT NULL,
    role public.user_role NOT NULL DEFAULT 'viewer',
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone
);

CREATE TABLE public.products (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    description text,
    price numeric(10, 2) NOT NULL CHECK (price >= 0),
    stock_count integer NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE public.orders (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id integer NOT NULL REFERENCES public.users(id),
    status public.order_status NOT NULL DEFAULT 'pending',
    total numeric(10, 2) NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    shipped_at timestamp with time zone
);

CREATE TABLE public.order_items (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id integer NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
    product_id integer NOT NULL REFERENCES public.products(id),
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(10, 2) NOT NULL
);

-- Additional indexes
CREATE INDEX idx_orders_user_id ON public.orders(user_id);
CREATE INDEX idx_orders_status ON public.orders(status);
CREATE INDEX idx_order_items_order_id ON public.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON public.order_items(product_id);

-- ============================================================
-- Sequences (standalone)
-- ============================================================
CREATE SEQUENCE public.invoice_number_seq START WITH 1000 INCREMENT BY 1;

-- ============================================================
-- Views
-- ============================================================
CREATE VIEW public.active_users AS
    SELECT id, username, email, role, created_at
    FROM public.users
    WHERE role != 'viewer';

-- ============================================================
-- Materialized Views
-- ============================================================
CREATE MATERIALIZED VIEW analytics.daily_order_summary AS
    SELECT
        date_trunc('day', o.created_at) AS order_date,
        count(*) AS order_count,
        sum(o.total) AS total_revenue
    FROM public.orders o
    GROUP BY date_trunc('day', o.created_at)
WITH DATA;

-- ============================================================
-- Functions
-- ============================================================
CREATE OR REPLACE FUNCTION public.calculate_order_total(p_order_id integer)
RETURNS numeric AS $$
    SELECT COALESCE(SUM(quantity * unit_price), 0)
    FROM public.order_items
    WHERE order_id = p_order_id;
$$ LANGUAGE sql STABLE;

-- ============================================================
-- Triggers
-- ============================================================
CREATE OR REPLACE FUNCTION public.update_order_total()
RETURNS trigger AS $$
BEGIN
    UPDATE public.orders
    SET total = public.calculate_order_total(NEW.order_id)
    WHERE id = NEW.order_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_order_total
    AFTER INSERT OR UPDATE ON public.order_items
    FOR EACH ROW EXECUTE FUNCTION public.update_order_total();

-- ============================================================
-- Roles & Permissions
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_role') THEN
        CREATE ROLE readonly_role;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_role') THEN
        CREATE ROLE app_role;
    END IF;
END $$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.users, public.orders, public.order_items TO app_role;
GRANT SELECT ON public.products TO app_role;
GRANT USAGE ON SCHEMA analytics TO readonly_role;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO readonly_role;

-- ============================================================
-- Sample Data
-- ============================================================
INSERT INTO public.users (username, email, role) VALUES
    ('alice', 'alice@example.com', 'admin'),
    ('bob', 'bob@example.com', 'editor'),
    ('charlie', 'charlie@example.com', 'viewer'),
    ('diana', 'diana@example.com', 'editor'),
    ('eve', 'eve@example.com', 'viewer');

INSERT INTO public.products (name, description, price, stock_count) VALUES
    ('Widget A', 'A standard widget', 9.99, 100),
    ('Widget B', 'A premium widget', 24.99, 50),
    ('Gadget X', 'An advanced gadget', 49.99, 25),
    ('Gadget Y', 'A basic gadget', 14.99, 200);

INSERT INTO public.orders (user_id, status, total, created_at) VALUES
    (1, 'delivered', 0, '2026-01-15 10:30:00+00'),
    (2, 'shipped', 0, '2026-02-20 14:00:00+00'),
    (1, 'pending', 0, '2026-03-01 09:15:00+00'),
    (3, 'processing', 0, '2026-03-10 16:45:00+00');

INSERT INTO public.order_items (order_id, quantity, unit_price, product_id) VALUES
    (1, 2, 9.99, 1),
    (1, 1, 49.99, 3),
    (2, 3, 24.99, 2),
    (3, 1, 14.99, 4),
    (4, 2, 9.99, 1),
    (4, 1, 24.99, 2);

-- Refresh materialized view with data
REFRESH MATERIALIZED VIEW analytics.daily_order_summary;

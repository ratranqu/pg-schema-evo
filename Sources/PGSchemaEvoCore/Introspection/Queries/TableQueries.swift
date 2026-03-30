/// SQL queries for introspecting table metadata from pg_catalog.
enum TableQueries {
    /// Query columns for a table. Bind: $1 = schema, $2 = table name.
    static let columns = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull AS is_nullable,
            pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS column_default,
            a.attnum AS ordinal_position,
            CASE WHEN a.attidentity != '' THEN true ELSE false END AS is_identity,
            CASE WHEN a.attidentity = 'a' THEN 'ALWAYS'
                 WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                 ELSE NULL END AS identity_generation
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE n.nspname = $1
          AND c.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """

    /// Query constraints for a table. Bind: $1 = schema, $2 = table name.
    static let constraints = """
        SELECT
            con.conname AS constraint_name,
            con.contype AS constraint_type,
            pg_catalog.pg_get_constraintdef(con.oid, true) AS definition,
            CASE WHEN con.contype = 'f' THEN
                (SELECT nsp.nspname || '.' || rel.relname
                 FROM pg_catalog.pg_class rel
                 JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
                 WHERE rel.oid = con.confrelid)
            ELSE NULL END AS referenced_table
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
        ORDER BY
            CASE con.contype
                WHEN 'p' THEN 1
                WHEN 'u' THEN 2
                WHEN 'f' THEN 3
                WHEN 'c' THEN 4
                WHEN 'x' THEN 5
            END,
            con.conname
        """

    /// Query indexes for a table. Bind: $1 = schema, $2 = table name.
    static let indexes = """
        SELECT
            i.relname AS index_name,
            pg_catalog.pg_get_indexdef(i.oid) AS definition,
            ix.indisunique AS is_unique,
            ix.indisprimary AS is_primary
        FROM pg_catalog.pg_index ix
        JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
        JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1
          AND t.relname = $2
        ORDER BY i.relname
        """

    /// Query triggers for a table. Bind: $1 = schema, $2 = table name.
    static let triggers = """
        SELECT
            t.tgname AS trigger_name,
            pg_catalog.pg_get_triggerdef(t.oid, true) AS definition
        FROM pg_catalog.pg_trigger t
        JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = $2
          AND NOT t.tgisinternal
        ORDER BY t.tgname
        """

    /// Query relation size. Bind: $1 = schema, $2 = table name.
    static let relationSize = """
        SELECT pg_catalog.pg_total_relation_size(c.oid)::bigint AS size_bytes
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        """

    /// Query table permissions. Bind: $1 = schema, $2 = table name.
    static let permissions = """
        SELECT
            grantee,
            privilege_type,
            is_grantable::boolean
        FROM information_schema.role_table_grants
        WHERE table_schema = $1
          AND table_name = $2
          AND grantor != grantee
        ORDER BY grantee, privilege_type
        """

    /// List tables in a schema. Bind: $1 = schema (nullable for all schemas).
    static let listTables = """
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND ($1::text IS NULL OR n.nspname = $1)
        ORDER BY n.nspname, c.relname
        """
}

import Foundation
import PostgresNIO
import Logging

/// PostgresNIO-based implementation of schema introspection.
public final class PGCatalogIntrospector: SchemaIntrospector, @unchecked Sendable {
    private let connection: PostgresConnection
    private let logger: Logger

    public init(connection: PostgresConnection, logger: Logger) {
        self.connection = connection
        self.logger = logger
    }

    // MARK: - Table

    public func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Table requires a schema: \(id)")
        }

        logger.debug("Introspecting table \(id)")

        let columns = try await queryColumns(schema: schema, name: id.name)
        if columns.isEmpty {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        let constraints = try await queryConstraints(schema: schema, name: id.name)
        let indexes = try await queryIndexes(schema: schema, name: id.name)
        let triggers = try await queryTriggers(schema: schema, name: id.name)

        return TableMetadata(
            id: id,
            columns: columns,
            constraints: constraints,
            indexes: indexes,
            triggers: triggers
        )
    }

    // MARK: - View

    public func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("View requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT pg_catalog.pg_get_viewdef(c.oid, true) AS definition
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND c.relkind = 'v'
            """

        let rows = try await connection.query(query, logger: logger)
        var definition: String?
        for try await row in rows {
            definition = try row.decode(String.self)
        }

        guard let def = definition else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        let columns = try await queryColumns(schema: schema, name: id.name)
        return ViewMetadata(id: id, definition: def, columns: columns)
    }

    // MARK: - Materialized View

    public func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Materialized view requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT pg_catalog.pg_get_viewdef(c.oid, true) AS definition
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND c.relkind = 'm'
            """

        let rows = try await connection.query(query, logger: logger)
        var definition: String?
        for try await row in rows {
            definition = try row.decode(String.self)
        }

        guard let def = definition else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        let columns = try await queryColumns(schema: schema, name: id.name)
        let indexes = try await queryIndexes(schema: schema, name: id.name)
        return MaterializedViewMetadata(id: id, definition: def, columns: columns, indexes: indexes)
    }

    // MARK: - Sequence

    public func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Sequence requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT
                s.data_type,
                s.start_value::bigint,
                s.increment::bigint,
                s.minimum_value::bigint,
                s.maximum_value::bigint,
                s.cycle_option,
                pg_catalog.pg_sequence.seqcache
            FROM information_schema.sequences s
            JOIN pg_catalog.pg_class c ON c.relname = s.sequence_name
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.sequence_schema
            JOIN pg_catalog.pg_sequence ON pg_catalog.pg_sequence.seqrelid = c.oid
            WHERE s.sequence_schema = \(schema)
              AND s.sequence_name = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let randomAccess = row.makeRandomAccess()
            let dataType = try randomAccess[0].decode(String.self)
            let startValue = try randomAccess[1].decode(Int64.self)
            let increment = try randomAccess[2].decode(Int64.self)
            let minValue = try randomAccess[3].decode(Int64.self)
            let maxValue = try randomAccess[4].decode(Int64.self)
            let cycleOption = try randomAccess[5].decode(String.self)
            let cacheSize = try randomAccess[6].decode(Int64.self)

            // Check if owned by a column
            let ownedBy = try await querySequenceOwner(schema: schema, name: id.name)

            return SequenceMetadata(
                id: id,
                dataType: dataType,
                startValue: startValue,
                increment: increment,
                minValue: minValue,
                maxValue: maxValue,
                cacheSize: cacheSize,
                isCycled: cycleOption == "YES",
                ownedByColumn: ownedBy
            )
        }

        throw PGSchemaEvoError.objectNotFound(id)
    }

    private func querySequenceOwner(schema: String, name: String) async throws -> String? {
        let query: PostgresQuery = """
            SELECT
                ns.nspname || '.' || cl.relname || '.' || a.attname AS owned_by
            FROM pg_catalog.pg_class seq
            JOIN pg_catalog.pg_namespace sn ON sn.oid = seq.relnamespace
            JOIN pg_catalog.pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
            JOIN pg_catalog.pg_class cl ON cl.oid = d.refobjid
            JOIN pg_catalog.pg_namespace ns ON ns.oid = cl.relnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = cl.oid AND a.attnum = d.refobjsubid
            WHERE sn.nspname = \(schema) AND seq.relname = \(name) AND seq.relkind = 'S'
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            return try row.decode(String.self)
        }
        return nil
    }

    // MARK: - Enum

    public func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Enum requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT e.enumlabel
            FROM pg_catalog.pg_enum e
            JOIN pg_catalog.pg_type t ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = \(schema)
              AND t.typname = \(id.name)
            ORDER BY e.enumsortorder
            """

        let rows = try await connection.query(query, logger: logger)
        var labels: [String] = []
        for try await row in rows {
            let label = try row.decode(String.self)
            labels.append(label)
        }

        guard !labels.isEmpty else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return EnumMetadata(id: id, labels: labels)
    }

    // MARK: - Function / Procedure

    public func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Function requires a schema: \(id)")
        }

        let kind: String
        switch id.type {
        case .procedure: kind = "p"
        default: kind = "f"
        }

        let query: PostgresQuery = """
            SELECT
                pg_catalog.pg_get_functiondef(p.oid) AS definition,
                l.lanname AS language,
                pg_catalog.pg_get_function_result(p.oid) AS return_type,
                p.proisstrict AS is_strict,
                p.provolatile AS volatility,
                p.prosecdef AS is_security_definer,
                pg_catalog.pg_get_function_identity_arguments(p.oid) AS arg_signature
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
            JOIN pg_catalog.pg_language l ON l.oid = p.prolang
            WHERE n.nspname = \(schema)
              AND p.proname = \(id.name)
              AND p.prokind = \(kind)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let randomAccess = row.makeRandomAccess()
            let definition = try randomAccess[0].decode(String.self)
            let language = try randomAccess[1].decode(String.self)
            let returnType = try randomAccess[2].decode(String?.self)
            let isStrict = try randomAccess[3].decode(Bool.self)
            let volatilityChar = try randomAccess[4].decode(String.self)
            let isSecurityDefiner = try randomAccess[5].decode(Bool.self)
            let argSignature = try randomAccess[6].decode(String.self)

            let volatility: String
            switch volatilityChar {
            case "i": volatility = "IMMUTABLE"
            case "s": volatility = "STABLE"
            default: volatility = "VOLATILE"
            }

            return FunctionMetadata(
                id: id,
                definition: definition,
                language: language,
                returnType: returnType,
                isStrict: isStrict,
                volatility: volatility,
                isSecurityDefiner: isSecurityDefiner,
                argumentSignature: argSignature
            )
        }

        throw PGSchemaEvoError.objectNotFound(id)
    }

    // MARK: - Schema

    public func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        let query: PostgresQuery = """
            SELECT r.rolname AS owner
            FROM pg_catalog.pg_namespace n
            JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
            WHERE n.nspname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let owner = try row.decode(String.self)
            return SchemaMetadata(id: id, owner: owner)
        }

        throw PGSchemaEvoError.objectNotFound(id)
    }

    // MARK: - Role

    public func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        let query: PostgresQuery = """
            SELECT
                r.rolcanlogin,
                r.rolsuper,
                r.rolcreatedb,
                r.rolcreaterole,
                r.rolconnlimit
            FROM pg_catalog.pg_roles r
            WHERE r.rolname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let randomAccess = row.makeRandomAccess()
            let canLogin = try randomAccess[0].decode(Bool.self)
            let isSuperuser = try randomAccess[1].decode(Bool.self)
            let canCreateDB = try randomAccess[2].decode(Bool.self)
            let canCreateRole = try randomAccess[3].decode(Bool.self)
            let connLimit = try randomAccess[4].decode(Int.self)

            // Get role memberships
            let memberOf = try await queryRoleMembership(roleName: id.name)

            return RoleMetadata(
                id: id,
                canLogin: canLogin,
                isSuperuser: isSuperuser,
                canCreateDB: canCreateDB,
                canCreateRole: canCreateRole,
                connectionLimit: connLimit,
                memberOf: memberOf
            )
        }

        throw PGSchemaEvoError.objectNotFound(id)
    }

    private func queryRoleMembership(roleName: String) async throws -> [String] {
        let query: PostgresQuery = """
            SELECT r.rolname
            FROM pg_catalog.pg_auth_members m
            JOIN pg_catalog.pg_roles r ON r.oid = m.roleid
            JOIN pg_catalog.pg_roles mr ON mr.oid = m.member
            WHERE mr.rolname = \(roleName)
            """

        let rows = try await connection.query(query, logger: logger)
        var roles: [String] = []
        for try await row in rows {
            let role = try row.decode(String.self)
            roles.append(role)
        }
        return roles
    }

    // MARK: - Composite Type

    public func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Composite type requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT
                a.attname AS attr_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                a.attnum::integer AS ordinal_position
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_catalog.pg_attribute a ON a.attrelid = t.typrelid
            WHERE n.nspname = \(schema)
              AND t.typname = \(id.name)
              AND t.typtype = 'c'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """

        let rows = try await connection.query(query, logger: logger)
        var attributes: [CompositeTypeAttribute] = []
        for try await row in rows {
            let (name, dataType, position) = try row.decode((String, String, Int).self)
            attributes.append(CompositeTypeAttribute(
                name: name,
                dataType: dataType,
                ordinalPosition: position
            ))
        }

        guard !attributes.isEmpty else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return CompositeTypeMetadata(id: id, attributes: attributes)
    }

    // MARK: - Extension

    public func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        let query: PostgresQuery = """
            SELECT
                e.extversion,
                n.nspname AS schema_name
            FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (version, schemaName) = try row.decode((String, String).self)
            return ExtensionMetadata(id: id, version: version, installedSchema: schemaName)
        }

        throw PGSchemaEvoError.objectNotFound(id)
    }

    // MARK: - Relation Size

    public func relationSize(_ id: ObjectIdentifier) async throws -> Int? {
        guard let schema = id.schema else { return nil }

        let query: PostgresQuery = """
            SELECT pg_catalog.pg_total_relation_size(c.oid)::bigint AS size_bytes
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema) AND c.relname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let sizeBytes = try row.decode(Int.self)
            return sizeBytes
        }
        return nil
    }

    // MARK: - List Objects

    public func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let targetTypes = types ?? ObjectType.allCases

        for type in targetTypes {
            switch type {
            case .table:
                try await results.append(contentsOf: listRelations(schema: schema, relkind: "r", type: .table))
            case .view:
                try await results.append(contentsOf: listRelations(schema: schema, relkind: "v", type: .view))
            case .materializedView:
                try await results.append(contentsOf: listRelations(schema: schema, relkind: "m", type: .materializedView))
            case .sequence:
                try await results.append(contentsOf: listRelations(schema: schema, relkind: "S", type: .sequence))
            case .enum:
                try await results.append(contentsOf: listEnums(schema: schema))
            case .function:
                try await results.append(contentsOf: listProcedures(schema: schema, kind: "f", type: .function))
            case .procedure:
                try await results.append(contentsOf: listProcedures(schema: schema, kind: "p", type: .procedure))
            case .aggregate:
                try await results.append(contentsOf: listProcedures(schema: schema, kind: "a", type: .aggregate))
            case .schema:
                try await results.append(contentsOf: listSchemas())
            case .role:
                try await results.append(contentsOf: listRoles())
            case .extension:
                try await results.append(contentsOf: listExtensions())
            case .foreignTable:
                try await results.append(contentsOf: listRelations(schema: schema, relkind: "f", type: .foreignTable))
            case .foreignDataWrapper:
                try await results.append(contentsOf: listForeignDataWrappers())
            case .compositeType:
                try await results.append(contentsOf: listCompositeTypes(schema: schema))
            case .operator:
                logger.debug("Listing not yet implemented for type: \(type.displayName)")
            }
        }

        return results
    }

    // MARK: - Permissions

    public func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] {
        guard let schema = id.schema else { return [] }

        let query: PostgresQuery = """
            SELECT grantee, privilege_type, is_grantable::boolean
            FROM information_schema.role_table_grants
            WHERE table_schema = \(schema)
              AND table_name = \(id.name)
              AND grantor != grantee
            ORDER BY grantee, privilege_type
            """

        let rows = try await connection.query(query, logger: logger)
        var grants: [PermissionGrant] = []
        for try await row in rows {
            let (grantee, privilege, isGrantable) = try row.decode((String, String, Bool).self)
            grants.append(PermissionGrant(
                grantee: grantee,
                privilege: privilege,
                isGrantable: isGrantable
            ))
        }
        return grants
    }

    // MARK: - Dependencies (pg_depend)

    public func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] {
        guard let schema = id.schema else { return [] }

        let query: PostgresQuery = """
            SELECT DISTINCT
                CASE dep_c.relkind
                    WHEN 'r' THEN 'table'
                    WHEN 'v' THEN 'view'
                    WHEN 'm' THEN 'matview'
                    WHEN 'S' THEN 'sequence'
                    WHEN 'f' THEN 'foreign_table'
                    ELSE 'table'
                END AS dep_type,
                dep_ns.nspname AS dep_schema,
                dep_c.relname AS dep_name
            FROM pg_catalog.pg_depend d
            JOIN pg_catalog.pg_class c ON c.oid = d.objid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class dep_c ON dep_c.oid = d.refobjid
            JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep_c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND d.deptype IN ('n', 'a')
              AND dep_c.relname != \(id.name)
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            UNION
            SELECT DISTINCT
                'enum' AS dep_type,
                dep_ns.nspname AS dep_schema,
                dep_t.typname AS dep_name
            FROM pg_catalog.pg_depend d
            JOIN pg_catalog.pg_class c ON c.oid = d.objid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_type dep_t ON dep_t.oid = d.refobjid AND dep_t.typtype = 'e'
            JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep_t.typnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND d.deptype IN ('n', 'a')
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            UNION
            SELECT DISTINCT
                'function' AS dep_type,
                dep_ns.nspname AS dep_schema,
                dep_p.proname AS dep_name
            FROM pg_catalog.pg_depend d
            JOIN pg_catalog.pg_class c ON c.oid = d.objid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_proc dep_p ON dep_p.oid = d.refobjid
            JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep_p.pronamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND d.deptype IN ('n', 'a')
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            UNION
            SELECT DISTINCT
                'table' AS dep_type,
                ref_ns.nspname AS dep_schema,
                ref_c.relname AS dep_name
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class ref_c ON ref_c.oid = con.confrelid
            JOIN pg_catalog.pg_namespace ref_ns ON ref_ns.oid = ref_c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND con.contype = 'f'
              AND ref_c.relname != \(id.name)
              AND ref_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            UNION
            SELECT DISTINCT
                CASE dep_c.relkind
                    WHEN 'r' THEN 'table'
                    WHEN 'v' THEN 'view'
                    WHEN 'm' THEN 'matview'
                    WHEN 'S' THEN 'sequence'
                    WHEN 'f' THEN 'foreign_table'
                    ELSE 'table'
                END AS dep_type,
                dep_ns.nspname AS dep_schema,
                dep_c.relname AS dep_name
            FROM pg_catalog.pg_depend d
            JOIN pg_catalog.pg_rewrite rw ON rw.oid = d.objid
            JOIN pg_catalog.pg_class c ON c.oid = rw.ev_class
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_class dep_c ON dep_c.oid = d.refobjid
            JOIN pg_catalog.pg_namespace dep_ns ON dep_ns.oid = dep_c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND dep_c.relname != \(id.name)
              AND dep_ns.nspname NOT IN ('pg_catalog', 'information_schema')
            """

        let rows = try await connection.query(query, logger: logger)
        var deps: [ObjectIdentifier] = []
        for try await row in rows {
            let (typeStr, depSchema, depName) = try row.decode((String, String, String).self)
            if let objType = ObjectType(rawValue: typeStr) {
                deps.append(ObjectIdentifier(type: objType, schema: depSchema, name: depName))
            }
        }
        return deps
    }

    // MARK: - Private listing helpers

    private func listRelations(schema: String?, relkind: String, type: ObjectType) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let query: PostgresQuery
        if let schema {
            query = """
                SELECT n.nspname, c.relname
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = \(relkind)
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND n.nspname = \(schema)
                ORDER BY n.nspname, c.relname
                """
        } else {
            query = """
                SELECT n.nspname, c.relname
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = \(relkind)
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY n.nspname, c.relname
                """
        }

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (schemaName, name) = try row.decode((String, String).self)
            results.append(ObjectIdentifier(type: type, schema: schemaName, name: name))
        }
        return results
    }

    private func listEnums(schema: String?) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let query: PostgresQuery
        if let schema {
            query = """
                SELECT n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'e'
                  AND n.nspname = \(schema)
                ORDER BY n.nspname, t.typname
                """
        } else {
            query = """
                SELECT n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'e'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, t.typname
                """
        }

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (schemaName, name) = try row.decode((String, String).self)
            results.append(ObjectIdentifier(type: .enum, schema: schemaName, name: name))
        }
        return results
    }

    private func listProcedures(schema: String?, kind: String, type: ObjectType) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let query: PostgresQuery
        if let schema {
            query = """
                SELECT n.nspname, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid) AS args
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE p.prokind = \(kind)
                  AND n.nspname = \(schema)
                ORDER BY n.nspname, p.proname
                """
        } else {
            query = """
                SELECT n.nspname, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid) AS args
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE p.prokind = \(kind)
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, p.proname
                """
        }

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (schemaName, name, args) = try row.decode((String, String, String).self)
            let sig = args.isEmpty ? nil : "(\(args))"
            results.append(ObjectIdentifier(type: type, schema: schemaName, name: name, signature: sig))
        }
        return results
    }

    private func listSchemas() async throws -> [ObjectIdentifier] {
        let query: PostgresQuery = """
            SELECT n.nspname
            FROM pg_catalog.pg_namespace n
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND n.nspname NOT LIKE 'pg_temp_%'
              AND n.nspname NOT LIKE 'pg_toast_temp_%'
            ORDER BY n.nspname
            """

        let rows = try await connection.query(query, logger: logger)
        var results: [ObjectIdentifier] = []
        for try await row in rows {
            let name = try row.decode(String.self)
            results.append(ObjectIdentifier(type: .schema, name: name))
        }
        return results
    }

    private func listRoles() async throws -> [ObjectIdentifier] {
        let query: PostgresQuery = """
            SELECT r.rolname
            FROM pg_catalog.pg_roles r
            WHERE r.rolname NOT LIKE 'pg_%'
              AND r.rolname != 'postgres'
            ORDER BY r.rolname
            """

        let rows = try await connection.query(query, logger: logger)
        var results: [ObjectIdentifier] = []
        for try await row in rows {
            let name = try row.decode(String.self)
            results.append(ObjectIdentifier(type: .role, name: name))
        }
        return results
    }

    private func listExtensions() async throws -> [ObjectIdentifier] {
        let query: PostgresQuery = """
            SELECT e.extname
            FROM pg_catalog.pg_extension e
            WHERE e.extname != 'plpgsql'
            ORDER BY e.extname
            """

        let rows = try await connection.query(query, logger: logger)
        var results: [ObjectIdentifier] = []
        for try await row in rows {
            let name = try row.decode(String.self)
            results.append(ObjectIdentifier(type: .extension, name: name))
        }
        return results
    }

    private func listCompositeTypes(schema: String?) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let query: PostgresQuery
        if let schema {
            query = """
                SELECT n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'c'
                  AND n.nspname = \(schema)
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_class c
                      WHERE c.oid = t.typrelid AND c.relkind != 'c'
                  )
                ORDER BY n.nspname, t.typname
                """
        } else {
            query = """
                SELECT n.nspname, t.typname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typtype = 'c'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_class c
                      WHERE c.oid = t.typrelid AND c.relkind != 'c'
                  )
                ORDER BY n.nspname, t.typname
                """
        }

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (schemaName, name) = try row.decode((String, String).self)
            results.append(ObjectIdentifier(type: .compositeType, schema: schemaName, name: name))
        }
        return results
    }

    private func listForeignDataWrappers() async throws -> [ObjectIdentifier] {
        let query: PostgresQuery = """
            SELECT fdwname
            FROM pg_catalog.pg_foreign_data_wrapper
            ORDER BY fdwname
            """

        let rows = try await connection.query(query, logger: logger)
        var results: [ObjectIdentifier] = []
        for try await row in rows {
            let name = try row.decode(String.self)
            results.append(ObjectIdentifier(type: .foreignDataWrapper, name: name))
        }
        return results
    }

    // MARK: - RLS Policies

    public func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo {
        guard let schema = id.schema else { return RLSInfo() }

        // Check if RLS is enabled
        let rlsQuery: PostgresQuery = """
            SELECT c.relrowsecurity, c.relforcerowsecurity
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
            """

        let rlsRows = try await connection.query(rlsQuery, logger: logger)
        var isEnabled = false
        var isForced = false
        for try await row in rlsRows {
            let (enabled, forced) = try row.decode((Bool, Bool).self)
            isEnabled = enabled
            isForced = forced
        }

        // Get policies
        let policyQuery: PostgresQuery = """
            SELECT
                pol.polname AS policy_name,
                pg_catalog.pg_get_expr(pol.polqual, pol.polrelid, true) AS policy_qual,
                pol.polcmd AS command,
                pol.polpermissive AS permissive,
                ARRAY(
                    SELECT r.rolname FROM pg_catalog.pg_roles r
                    WHERE r.oid = ANY(pol.polroles)
                )::text[] AS roles,
                pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid, true) AS with_check
            FROM pg_catalog.pg_policy pol
            JOIN pg_catalog.pg_class c ON c.oid = pol.polrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
            ORDER BY pol.polname
            """

        let policyRows = try await connection.query(policyQuery, logger: logger)
        var policies: [RLSPolicy] = []
        for try await row in policyRows {
            let randomAccess = row.makeRandomAccess()
            let name = try randomAccess[0].decode(String.self)
            let qual = try randomAccess[1].decode(String?.self)
            let cmd = try randomAccess[2].decode(String.self)
            let permissive = try randomAccess[3].decode(Bool.self)
            let roles = try randomAccess[4].decode([String].self)
            let withCheck = try randomAccess[5].decode(String?.self)

            let cmdStr: String
            switch cmd {
            case "r": cmdStr = "SELECT"
            case "a": cmdStr = "INSERT"
            case "w": cmdStr = "UPDATE"
            case "d": cmdStr = "DELETE"
            default: cmdStr = "ALL"
            }

            let permStr = permissive ? "PERMISSIVE" : "RESTRICTIVE"

            var definition = "CREATE POLICY \(quoteIdent(name)) ON \(id.qualifiedName)"
            definition += " AS \(permStr)"
            definition += " FOR \(cmdStr)"
            if !roles.isEmpty {
                definition += " TO \(roles.joined(separator: ", "))"
            }
            if let q = qual {
                definition += "\n    USING (\(q))"
            }
            if let wc = withCheck {
                definition += "\n    WITH CHECK (\(wc))"
            }
            definition += ";"

            policies.append(RLSPolicy(name: name, definition: definition))
        }

        return RLSInfo(isEnabled: isEnabled, isForced: isForced, policies: policies)
    }

    // MARK: - Partition Info

    public func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? {
        guard let schema = id.schema else { return nil }

        let query: PostgresQuery = """
            SELECT
                CASE pt.partstrat
                    WHEN 'r' THEN 'RANGE'
                    WHEN 'l' THEN 'LIST'
                    WHEN 'h' THEN 'HASH'
                END AS strategy,
                pg_catalog.pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = c.oid
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let (strategy, partKey) = try row.decode((String, String).self)
            return PartitionInfo(strategy: strategy, partitionKey: partKey)
        }
        return nil
    }

    public func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] {
        guard let schema = id.schema else { return [] }

        let query: PostgresQuery = """
            SELECT
                child_ns.nspname AS child_schema,
                child_c.relname AS child_name,
                pg_catalog.pg_get_expr(child_c.relpartbound, child_c.oid) AS bound_spec
            FROM pg_catalog.pg_inherits inh
            JOIN pg_catalog.pg_class parent_c ON parent_c.oid = inh.inhparent
            JOIN pg_catalog.pg_namespace parent_ns ON parent_ns.oid = parent_c.relnamespace
            JOIN pg_catalog.pg_class child_c ON child_c.oid = inh.inhrelid
            JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child_c.relnamespace
            WHERE parent_ns.nspname = \(schema)
              AND parent_c.relname = \(id.name)
            ORDER BY child_c.relname
            """

        let rows = try await connection.query(query, logger: logger)
        var partitions: [PartitionChild] = []
        for try await row in rows {
            let (childSchema, childName, boundSpec) = try row.decode((String, String, String).self)
            let childId = ObjectIdentifier(type: .table, schema: childSchema, name: childName)
            partitions.append(PartitionChild(id: childId, boundSpec: boundSpec))
        }
        return partitions
    }

    // MARK: - Primary Key Columns

    public func primaryKeyColumns(for id: ObjectIdentifier) async throws -> [String] {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Table requires a schema: \(id)")
        }

        let query: PostgresQuery = """
            SELECT a.attname
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_attribute a
              ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(id.name)
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """

        let rows = try await connection.query(query, logger: logger)
        var columns: [String] = []
        for try await row in rows {
            let name = try row.decode(String.self)
            columns.append(name)
        }
        return columns
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    // MARK: - Private column/constraint/index/trigger query helpers

    public func queryColumns(schema: String, name: String) async throws -> [ColumnInfo] {
        let query: PostgresQuery = """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull AS is_nullable,
                pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS column_default,
                a.attnum::integer AS ordinal_position,
                CASE WHEN a.attidentity != '' THEN true ELSE false END AS is_identity,
                CASE WHEN a.attidentity = 'a' THEN 'ALWAYS'
                     WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                     ELSE NULL END AS identity_generation,
                information_schema._pg_char_max_length(a.atttypid, a.atttypmod)::integer AS character_maximum_length,
                information_schema._pg_numeric_precision(a.atttypid, a.atttypmod)::integer AS numeric_precision,
                information_schema._pg_numeric_scale(a.atttypid, a.atttypmod)::integer AS numeric_scale
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """

        let rows = try await connection.query(query, logger: logger)
        var columns: [ColumnInfo] = []
        for try await row in rows {
            let randomAccess = row.makeRandomAccess()
            let columnName = try randomAccess[0].decode(String.self)
            let dataType = try randomAccess[1].decode(String.self)
            let isNullable = try randomAccess[2].decode(Bool.self)
            let columnDefault = try randomAccess[3].decode(String?.self)
            let ordinalPosition = try randomAccess[4].decode(Int.self)
            let isIdentity = try randomAccess[5].decode(Bool.self)
            let identityGeneration = try randomAccess[6].decode(String?.self)
            let characterMaximumLength = try randomAccess[7].decode(Int?.self)
            let numericPrecision = try randomAccess[8].decode(Int?.self)
            let numericScale = try randomAccess[9].decode(Int?.self)
            columns.append(ColumnInfo(
                name: columnName,
                dataType: dataType,
                isNullable: isNullable,
                columnDefault: columnDefault,
                ordinalPosition: ordinalPosition,
                characterMaximumLength: characterMaximumLength,
                numericPrecision: numericPrecision,
                numericScale: numericScale,
                isIdentity: isIdentity,
                identityGeneration: identityGeneration
            ))
        }
        return columns
    }

    public func queryConstraints(schema: String, name: String) async throws -> [ConstraintInfo] {
        let query: PostgresQuery = """
            SELECT
                con.conname AS constraint_name,
                con.contype::text AS constraint_type,
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
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
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

        let rows = try await connection.query(query, logger: logger)
        var constraints: [ConstraintInfo] = []
        for try await row in rows {
            let (constraintName, constraintType, definition, referencedTable) =
                try row.decode((String, String, String, String?).self)
            if let type = ConstraintInfo.ConstraintType(rawValue: constraintType) {
                constraints.append(ConstraintInfo(
                    name: constraintName,
                    type: type,
                    definition: definition,
                    referencedTable: referencedTable
                ))
            }
        }
        return constraints
    }

    public func queryIndexes(schema: String, name: String) async throws -> [IndexInfo] {
        let query: PostgresQuery = """
            SELECT
                i.relname AS index_name,
                pg_catalog.pg_get_indexdef(i.oid) AS definition,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = \(schema)
              AND t.relname = \(name)
            ORDER BY i.relname
            """

        let rows = try await connection.query(query, logger: logger)
        var indexes: [IndexInfo] = []
        for try await row in rows {
            let (indexName, definition, isUnique, isPrimary) =
                try row.decode((String, String, Bool, Bool).self)
            indexes.append(IndexInfo(
                name: indexName,
                definition: definition,
                isUnique: isUnique,
                isPrimary: isPrimary
            ))
        }
        return indexes
    }

    public func queryTriggers(schema: String, name: String) async throws -> [TriggerInfo] {
        let query: PostgresQuery = """
            SELECT
                t.tgname AS trigger_name,
                pg_catalog.pg_get_triggerdef(t.oid, true) AS definition
            FROM pg_catalog.pg_trigger t
            JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
              AND NOT t.tgisinternal
            ORDER BY t.tgname
            """

        let rows = try await connection.query(query, logger: logger)
        var triggers: [TriggerInfo] = []
        for try await row in rows {
            let (triggerName, definition) = try row.decode((String, String).self)
            triggers.append(TriggerInfo(name: triggerName, definition: definition))
        }
        return triggers
    }
}

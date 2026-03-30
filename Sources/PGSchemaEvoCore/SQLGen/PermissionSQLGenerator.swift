/// Generates GRANT statements from permission metadata.
public struct PermissionSQLGenerator: Sendable {

    public init() {}

    /// Generate GRANT statements for the given object and permissions.
    public func generateGrants(for id: ObjectIdentifier, grants: [PermissionGrant]) -> String {
        guard !grants.isEmpty else { return "" }

        // Group by grantee for compact output
        var granteePrivileges: [(grantee: String, privileges: [String])] = []
        var currentGrantee = ""
        var currentPrivileges: [String] = []

        for grant in grants.sorted(by: { $0.grantee < $1.grantee }) {
            if grant.grantee != currentGrantee {
                if !currentGrantee.isEmpty {
                    granteePrivileges.append((currentGrantee, currentPrivileges))
                }
                currentGrantee = grant.grantee
                currentPrivileges = [grant.privilege]
            } else {
                currentPrivileges.append(grant.privilege)
            }
        }
        if !currentGrantee.isEmpty {
            granteePrivileges.append((currentGrantee, currentPrivileges))
        }

        let objectTypeSQL: String
        switch id.type {
        case .table, .view, .materializedView, .foreignTable:
            objectTypeSQL = "TABLE"
        case .sequence:
            objectTypeSQL = "SEQUENCE"
        case .function:
            objectTypeSQL = "FUNCTION"
        case .procedure:
            objectTypeSQL = "PROCEDURE"
        case .schema:
            objectTypeSQL = "SCHEMA"
        default:
            objectTypeSQL = "TABLE"
        }

        var sql = ""
        for (grantee, privileges) in granteePrivileges {
            let privList = privileges.joined(separator: ", ")
            let target: String
            if id.type == .function || id.type == .procedure {
                target = "\(id.qualifiedName)\(id.signature ?? "()")"
            } else {
                target = id.qualifiedName
            }
            sql += "GRANT \(privList) ON \(objectTypeSQL) \(target) TO \(quoteIdent(grantee));\n"
        }

        return sql
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}

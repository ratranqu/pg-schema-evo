import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("Selective Data Tests")
struct SelectiveDataTests {

    @Test("ObjectSpec supports WHERE clause")
    func objectSpecWhereClause() {
        let spec = ObjectSpec(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            copyData: true,
            whereClause: "status = 'pending'"
        )
        #expect(spec.whereClause == "status = 'pending'")
    }

    @Test("ObjectSpec supports row limit")
    func objectSpecRowLimit() {
        let spec = ObjectSpec(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            copyData: true,
            rowLimit: 1000
        )
        #expect(spec.rowLimit == 1000)
    }

    @Test("CloneJob supports global row limit")
    func cloneJobGlobalRowLimit() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: [],
            globalRowLimit: 5000
        )
        #expect(job.globalRowLimit == 5000)
    }

    @Test("CloneStep copyData carries WHERE and LIMIT")
    func cloneStepCopyDataWithFilters() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let step = CloneStep.copyData(
            id: id,
            method: .copy,
            estimatedSize: 1024,
            whereClause: "created_at > '2024-01-01'",
            rowLimit: 500
        )

        if case .copyData(_, _, _, let wh, let lim) = step {
            #expect(wh == "created_at > '2024-01-01'")
            #expect(lim == 500)
        } else {
            Issue.record("Expected copyData step")
        }
    }

    @Test("Config loader parses WHERE and row_limit")
    func configLoaderWhereAndLimit() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: table
                schema: public
                name: orders
                data: true
                where: "status = 'pending'"
                row_limit: 1000
              - type: table
                schema: public
                name: users
                data: true
                rls: true
            """
        let path = NSTemporaryDirectory() + "test-selective-\(UUID().uuidString).yaml"
        try yaml.write(toFile: path, atomically: true, encoding: .utf8)

        let loader = ConfigLoader()
        let config = try loader.load(path: path)

        #expect(config.objects[0].whereClause == "status = 'pending'")
        #expect(config.objects[0].rowLimit == 1000)
        #expect(config.objects[1].copyRLSPolicies == true)
    }
}

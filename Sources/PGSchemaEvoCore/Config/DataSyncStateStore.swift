import Foundation
import Yams

/// Loads and saves the incremental data sync state file.
///
/// The state file is a YAML file that tracks the last synced value per table,
/// enabling incremental change detection on subsequent runs.
public struct DataSyncStateStore: Sendable {

    public init() {}

    /// Load sync state from a YAML file.
    public func load(path: String) throws -> DataSyncState {
        let url = URL(fileURLWithPath: path)
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw PGSchemaEvoError.syncStateFileNotFound(path: path)
        }

        let raw: String
        do {
            raw = try String(contentsOf: url, encoding: .utf8)
        } catch {
            throw PGSchemaEvoError.syncStateCorrupted(path: path, underlying: error.localizedDescription)
        }

        guard let yaml = try Yams.load(yaml: raw) as? [String: Any] else {
            throw PGSchemaEvoError.syncStateCorrupted(path: path, underlying: "Expected a YAML mapping at root level")
        }

        guard let tablesDict = yaml["tables"] as? [String: Any] else {
            throw PGSchemaEvoError.syncStateCorrupted(path: path, underlying: "Missing 'tables' section")
        }

        var tables: [String: DataSyncTableState] = [:]
        for (key, value) in tablesDict {
            guard let entry = value as? [String: Any],
                  let column = entry["column"] as? String,
                  let lastValue = entry["last_value"] else {
                throw PGSchemaEvoError.syncStateCorrupted(
                    path: path,
                    underlying: "Invalid entry for table '\(key)'"
                )
            }
            tables[key] = DataSyncTableState(column: column, lastValue: "\(lastValue)")
        }

        return DataSyncState(tables: tables)
    }

    /// Save sync state to a YAML file.
    public func save(state: DataSyncState, path: String) throws {
        var tablesDict: [String: [String: String]] = [:]
        for (key, tableState) in state.tables.sorted(by: { $0.key < $1.key }) {
            tablesDict[key] = [
                "column": tableState.column,
                "last_value": tableState.lastValue,
            ]
        }

        let root: [String: Any] = ["tables": tablesDict]
        let yamlString = try Yams.dump(object: root, sortKeys: true)

        let url = URL(fileURLWithPath: path)
        try yamlString.write(to: url, atomically: true, encoding: .utf8)
    }
}

import Foundation
import Yams

/// Loads and parses YAML configuration files for clone jobs.
///
/// Supports environment variable interpolation using `${VAR_NAME}` syntax.
/// Missing environment variables cause a hard error unless a default is provided
/// with `${VAR_NAME:-default}`.
public struct ConfigLoader: Sendable {

    public init() {}

    /// Load a clone job from a YAML config file.
    ///
    /// CLI flags can override config values (non-nil overrides win).
    public func load(
        path: String,
        overrides: ConfigOverrides = ConfigOverrides()
    ) throws -> CloneJobConfig {
        let url = URL(fileURLWithPath: path)
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw PGSchemaEvoError.configFileNotFound(path: path)
        }

        let raw: String
        do {
            raw = try String(contentsOf: url, encoding: .utf8)
        } catch {
            throw PGSchemaEvoError.configParseError(path: path, underlying: error.localizedDescription)
        }

        // Interpolate environment variables
        let interpolated = try interpolateEnvVars(raw)

        guard let yaml = try Yams.load(yaml: interpolated) as? [String: Any] else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Expected a YAML mapping at root level")
        }

        return try parseConfig(yaml, path: path, overrides: overrides)
    }

    // MARK: - Environment Variable Interpolation

    /// Replace `${VAR}` and `${VAR:-default}` patterns with environment values.
    func interpolateEnvVars(_ input: String) throws -> String {
        // Pattern matches ${VAR_NAME} and ${VAR_NAME:-default_value}
        let pattern = #"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:[^}]|\}(?!\}))*))?\}"#
        let regex = try NSRegularExpression(pattern: pattern)
        let range = NSRange(input.startIndex..<input.endIndex, in: input)

        var result = input
        // Process matches in reverse order to preserve indices
        let matches = regex.matches(in: input, range: range).reversed()

        for match in matches {
            guard let varNameRange = Range(match.range(at: 1), in: input) else { continue }
            let varName = String(input[varNameRange])

            let matchRange = Range(match.range, in: result)!

            if let value = ProcessInfo.processInfo.environment[varName] {
                result.replaceSubrange(matchRange, with: value)
            } else if match.range(at: 2).location != NSNotFound,
                      let defaultRange = Range(match.range(at: 2), in: input) {
                let defaultValue = String(input[defaultRange])
                result.replaceSubrange(matchRange, with: defaultValue)
            } else {
                throw PGSchemaEvoError.undefinedEnvironmentVariable(name: varName)
            }
        }

        return result
    }

    // MARK: - Parsing

    private func parseConfig(
        _ yaml: [String: Any],
        path: String,
        overrides: ConfigOverrides
    ) throws -> CloneJobConfig {
        // Source connection
        guard let sourceDict = yaml["source"] as? [String: Any] else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Missing 'source' section")
        }
        let source = try parseConnection(sourceDict, label: "source", path: path)

        // Target connection
        guard let targetDict = yaml["target"] as? [String: Any] else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Missing 'target' section")
        }
        let target = try parseConnection(targetDict, label: "target", path: path)

        // Defaults
        let defaults = yaml["defaults"] as? [String: Any] ?? [:]
        let defaultPermissions = overrides.permissions ?? (defaults["permissions"] as? Bool) ?? false
        let defaultData = overrides.data ?? (defaults["data"] as? Bool) ?? false
        let defaultCascade = overrides.cascade ?? (defaults["cascade"] as? Bool) ?? false
        let defaultDataMethod = overrides.dataMethod ?? parseDataMethod(defaults["data_method"] as? String) ?? .auto
        let defaultDataThresholdMB = overrides.dataThresholdMB ?? (defaults["data_threshold_mb"] as? Int) ?? 100
        let defaultDropExisting = overrides.dropExisting ?? (defaults["drop_existing"] as? Bool) ?? false

        // Objects
        guard let objectDicts = yaml["objects"] as? [[String: Any]], !objectDicts.isEmpty else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Missing or empty 'objects' section")
        }

        var specs: [ObjectSpec] = []
        for (index, objDict) in objectDicts.enumerated() {
            let spec = try parseObjectSpec(
                objDict,
                index: index,
                path: path,
                defaultPermissions: defaultPermissions,
                defaultData: defaultData,
                defaultCascade: defaultCascade
            )
            specs.append(spec)
        }

        return CloneJobConfig(
            source: source,
            target: target,
            objects: specs,
            dryRun: overrides.dryRun ?? false,
            defaultDataMethod: defaultDataMethod,
            dataSizeThresholdMB: defaultDataThresholdMB,
            dropIfExists: defaultDropExisting,
            force: overrides.force ?? false
        )
    }

    private func parseConnection(
        _ dict: [String: Any],
        label: String,
        path: String
    ) throws -> ConnectionConfig {
        // Support either a DSN string or individual fields
        if let dsn = dict["dsn"] as? String {
            return try ConnectionConfig.fromDSN(dsn)
        }

        guard let host = dict["host"] as? String else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Missing 'host' in \(label) connection")
        }
        let port = dict["port"] as? Int ?? 5432
        guard let database = dict["database"] as? String else {
            throw PGSchemaEvoError.configParseError(path: path, underlying: "Missing 'database' in \(label) connection")
        }
        let username = dict["username"] as? String ?? "postgres"
        let password = dict["password"] as? String
        let sslStr = dict["ssl"] as? String ?? "disable"
        let sslMode = SSLMode(rawValue: sslStr) ?? .disable

        return ConnectionConfig(
            host: host,
            port: port,
            database: database,
            username: username,
            password: password,
            sslMode: sslMode
        )
    }

    private func parseObjectSpec(
        _ dict: [String: Any],
        index: Int,
        path: String,
        defaultPermissions: Bool,
        defaultData: Bool,
        defaultCascade: Bool
    ) throws -> ObjectSpec {
        guard let typeStr = dict["type"] as? String else {
            throw PGSchemaEvoError.configParseError(
                path: path,
                underlying: "Missing 'type' in object at index \(index)"
            )
        }
        guard let type = ObjectType(rawValue: typeStr) else {
            let validTypes = ObjectType.allCases.map(\.rawValue).joined(separator: ", ")
            throw PGSchemaEvoError.configParseError(
                path: path,
                underlying: "Invalid type '\(typeStr)' at index \(index). Valid: \(validTypes)"
            )
        }

        guard let name = dict["name"] as? String else {
            throw PGSchemaEvoError.configParseError(
                path: path,
                underlying: "Missing 'name' in object at index \(index)"
            )
        }

        let schema = dict["schema"] as? String ?? (type.isSchemaScoped ? "public" : nil)
        let signature = dict["signature"] as? String

        let id = ObjectIdentifier(type: type, schema: schema, name: name, signature: signature)

        let copyPermissions = (dict["permissions"] as? Bool) ?? defaultPermissions
        let copyData = (dict["data"] as? Bool) ?? (type.supportsData ? defaultData : false)
        let cascade = (dict["cascade"] as? Bool) ?? defaultCascade

        return ObjectSpec(
            id: id,
            copyPermissions: copyPermissions,
            copyData: copyData,
            cascadeDependencies: cascade
        )
    }

    private func parseDataMethod(_ str: String?) -> TransferMethod? {
        guard let str else { return nil }
        return TransferMethod(rawValue: str)
    }
}

/// Overrides from CLI flags that take precedence over config file values.
public struct ConfigOverrides: Sendable {
    public var dryRun: Bool?
    public var data: Bool?
    public var permissions: Bool?
    public var cascade: Bool?
    public var dataMethod: TransferMethod?
    public var dataThresholdMB: Int?
    public var dropExisting: Bool?
    public var force: Bool?

    public init(
        dryRun: Bool? = nil,
        data: Bool? = nil,
        permissions: Bool? = nil,
        cascade: Bool? = nil,
        dataMethod: TransferMethod? = nil,
        dataThresholdMB: Int? = nil,
        dropExisting: Bool? = nil,
        force: Bool? = nil
    ) {
        self.dryRun = dryRun
        self.data = data
        self.permissions = permissions
        self.cascade = cascade
        self.dataMethod = dataMethod
        self.dataThresholdMB = dataThresholdMB
        self.dropExisting = dropExisting
        self.force = force
    }
}

/// Parsed config file result, ready to be converted to a CloneJob.
public struct CloneJobConfig: Sendable {
    public let source: ConnectionConfig
    public let target: ConnectionConfig
    public let objects: [ObjectSpec]
    public let dryRun: Bool
    public let defaultDataMethod: TransferMethod
    public let dataSizeThresholdMB: Int
    public let dropIfExists: Bool
    public let force: Bool

    public func toCloneJob() -> CloneJob {
        CloneJob(
            source: source,
            target: target,
            objects: objects,
            dryRun: dryRun,
            defaultDataMethod: defaultDataMethod,
            dataSizeThreshold: dataSizeThresholdMB * 1024 * 1024,
            dropIfExists: dropIfExists
        )
    }
}

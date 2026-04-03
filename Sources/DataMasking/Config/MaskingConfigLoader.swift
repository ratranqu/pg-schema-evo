import Foundation
import Yams

/// Loads masking configuration from YAML files or strings.
///
/// This is a convenience layer on top of the fully programmatic `MaskingConfig` API.
public struct MaskingConfigLoader: Sendable {
    public init() {}

    /// Load configuration from a YAML file path.
    public func load(from path: String) throws -> MaskingConfig {
        let data: Data
        do {
            data = try Data(contentsOf: URL(fileURLWithPath: path))
        } catch {
            throw MaskingError.configLoadFailed(path: path, detail: error.localizedDescription)
        }
        guard let yaml = String(data: data, encoding: .utf8) else {
            throw MaskingError.configLoadFailed(path: path, detail: "File is not valid UTF-8")
        }
        return try loadFromString(yaml, path: path)
    }

    /// Load configuration from a YAML string.
    public func loadFromString(_ yaml: String, path: String = "<inline>") throws -> MaskingConfig {
        do {
            let decoder = YAMLDecoder()
            return try decoder.decode(MaskingConfig.self, from: yaml)
        } catch {
            throw MaskingError.configLoadFailed(path: path, detail: error.localizedDescription)
        }
    }
}

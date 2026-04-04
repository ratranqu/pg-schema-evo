import Foundation

/// JSON-based conflict file for offline review and resolution.
/// Users can generate a conflict file, review it, edit resolution choices,
/// then pass it back via `--resolve-from`.
public struct ConflictFileIO: Sendable {

    /// The on-disk format for a conflict file.
    struct ConflictFile: Codable {
        let generated: Date
        let conflicts: [ConflictEntry]
    }

    /// A single conflict entry in the file, with an editable resolution field.
    struct ConflictEntry: Codable {
        let id: UUID
        let object: String
        let kind: ConflictKind
        let description: String
        let isDestructive: Bool
        let isIrreversible: Bool
        let sourceSQL: [String]
        let targetSQL: [String]
        /// User fills this in: "apply-source", "keep-target", or "skip".
        /// Nil means unresolved.
        var resolution: ResolutionChoice?

        // Explicitly encode resolution as null so users can see the field to fill in
        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encode(id, forKey: .id)
            try container.encode(object, forKey: .object)
            try container.encode(kind, forKey: .kind)
            try container.encode(description, forKey: .description)
            try container.encode(isDestructive, forKey: .isDestructive)
            try container.encode(isIrreversible, forKey: .isIrreversible)
            try container.encode(sourceSQL, forKey: .sourceSQL)
            try container.encode(targetSQL, forKey: .targetSQL)
            try container.encodeIfPresent(resolution, forKey: .resolution)
            if resolution == nil {
                try container.encodeNil(forKey: .resolution)
            }
        }
    }

    /// Write a conflict report to a JSON file for offline review.
    public static func writeConflictFile(report: ConflictReport, to path: String) throws {
        let entries = report.conflicts.map { conflict in
            ConflictEntry(
                id: conflict.id,
                object: conflict.objectIdentifier,
                kind: conflict.kind,
                description: conflict.description,
                isDestructive: conflict.isDestructive,
                isIrreversible: conflict.isIrreversible,
                sourceSQL: conflict.sourceSQL,
                targetSQL: conflict.targetSQL,
                resolution: nil
            )
        }

        let file = ConflictFile(generated: report.detectedAt, conflicts: entries)

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(file)

        try data.write(to: URL(fileURLWithPath: path))
    }

    /// Read resolutions from a previously generated and edited conflict file.
    /// Returns resolutions only for entries where `resolution` is non-nil.
    public static func readResolutions(from path: String) throws -> [ConflictResolution] {
        let url = URL(fileURLWithPath: path)
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw PGSchemaEvoError.conflictFileParseError(path: path, underlying: error.localizedDescription)
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let file: ConflictFile
        do {
            file = try decoder.decode(ConflictFile.self, from: data)
        } catch {
            throw PGSchemaEvoError.conflictFileParseError(path: path, underlying: error.localizedDescription)
        }

        return file.conflicts.compactMap { entry in
            guard let choice = entry.resolution else { return nil }
            return ConflictResolution(
                conflictId: entry.id,
                choice: choice,
                timestamp: Date()
            )
        }
    }

    /// Match file-based resolutions against a current conflict report.
    /// Returns matched resolutions. Conflicts in the file that no longer exist
    /// are silently skipped. New conflicts not in the file are unresolved.
    public static func matchResolutions(
        fileResolutions: [ConflictResolution],
        report: ConflictReport
    ) -> (matched: [ConflictResolution], unresolved: [SchemaConflict]) {
        let currentIds = Set(report.conflicts.map(\.id))
        let matched = fileResolutions.filter { currentIds.contains($0.conflictId) }
        let resolvedIds = Set(matched.map(\.conflictId))
        let unresolved = report.conflicts.filter { !resolvedIds.contains($0.id) }
        return (matched, unresolved)
    }
}

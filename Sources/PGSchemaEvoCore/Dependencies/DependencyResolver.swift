/// Resolves object dependencies and produces a topologically sorted clone order.
///
/// When cascade mode is enabled, the resolver queries pg_depend for each requested
/// object and recursively discovers all dependencies. The final list is topologically
/// sorted so that dependencies are created before the objects that depend on them.
public struct DependencyResolver: Sendable {

    public init() {}

    /// Given a set of requested objects and an introspector, resolve all dependencies
    /// and return objects in topological (creation) order.
    ///
    /// - Parameters:
    ///   - objects: The explicitly requested objects.
    ///   - introspector: Used to query pg_depend for each object.
    ///   - cascade: If true, recursively discover dependencies. If false, just return objects as-is.
    /// - Returns: Objects in topological order (dependencies first).
    /// - Throws: `PGSchemaEvoError.dependencyCycle` if a cycle is detected.
    public func resolve(
        objects: [ObjectSpec],
        introspector: SchemaIntrospector,
        cascade: Bool
    ) async throws -> [ObjectSpec] {
        guard cascade else {
            return sortByTypeOrder(objects)
        }

        // Build the full dependency graph
        var graph: [ObjectIdentifier: Set<ObjectIdentifier>] = [:]
        var specMap: [ObjectIdentifier: ObjectSpec] = [:]
        var visited: Set<ObjectIdentifier> = []
        var queue: [ObjectSpec] = objects

        for spec in objects {
            specMap[spec.id] = spec
        }

        while !queue.isEmpty {
            let spec = queue.removeFirst()
            guard !visited.contains(spec.id) else { continue }
            visited.insert(spec.id)

            let deps = try await introspector.dependencies(for: spec.id)
            graph[spec.id] = Set(deps)

            for dep in deps {
                if !visited.contains(dep) {
                    let depSpec = ObjectSpec(
                        id: dep,
                        copyPermissions: spec.copyPermissions,
                        copyData: false,
                        cascadeDependencies: false
                    )
                    specMap[dep] = specMap[dep] ?? depSpec
                    queue.append(depSpec)
                }
            }
        }

        // Ensure all nodes appear in the graph
        for id in visited {
            if graph[id] == nil {
                graph[id] = []
            }
        }

        // Topological sort (Kahn's algorithm)
        let sorted = try topologicalSort(graph)

        return sorted.compactMap { specMap[$0] }
    }

    /// Topological sort using Kahn's algorithm.
    /// Returns nodes in dependency-first order (if A depends on B, B comes first).
    private func topologicalSort(_ graph: [ObjectIdentifier: Set<ObjectIdentifier>]) throws -> [ObjectIdentifier] {
        var inDegree: [ObjectIdentifier: Int] = [:]
        var adjacency: [ObjectIdentifier: [ObjectIdentifier]] = [:]

        for node in graph.keys {
            if inDegree[node] == nil { inDegree[node] = 0 }
            adjacency[node] = []
        }

        // Build reverse adjacency: if A depends on B, edge from B -> A
        for (node, deps) in graph {
            for dep in deps {
                if inDegree[dep] == nil { inDegree[dep] = 0 }
                adjacency[dep, default: []].append(node)
                inDegree[node, default: 0] += 1
            }
        }

        var queue: [ObjectIdentifier] = inDegree.filter { $0.value == 0 }.map(\.key)
        // Sort the initial queue by type order for deterministic output
        queue.sort { typeCreationOrder($0.type) < typeCreationOrder($1.type) }

        var result: [ObjectIdentifier] = []

        while !queue.isEmpty {
            let node = queue.removeFirst()
            result.append(node)

            for neighbor in adjacency[node, default: []] {
                inDegree[neighbor, default: 0] -= 1
                if inDegree[neighbor] == 0 {
                    queue.append(neighbor)
                }
            }
        }

        if result.count != inDegree.count {
            // Cycle detected — find participants
            let remaining = Set(inDegree.keys).subtracting(result)
            throw PGSchemaEvoError.dependencyCycle(participants: Array(remaining))
        }

        return result
    }

    /// Sort objects by their natural creation order (no dependency resolution).
    private func sortByTypeOrder(_ objects: [ObjectSpec]) -> [ObjectSpec] {
        objects.sorted { typeCreationOrder($0.id.type) < typeCreationOrder($1.id.type) }
    }

    /// The natural creation order for object types.
    /// Objects that other types depend on should be created first.
    private func typeCreationOrder(_ type: ObjectType) -> Int {
        switch type {
        case .role: 0
        case .schema: 1
        case .extension: 2
        case .foreignDataWrapper: 3
        case .enum: 4
        case .compositeType: 5
        case .sequence: 6
        case .table: 7
        case .foreignTable: 8
        case .view: 9
        case .materializedView: 10
        case .function: 11
        case .procedure: 12
        case .aggregate: 13
        case .operator: 14
        }
    }
}

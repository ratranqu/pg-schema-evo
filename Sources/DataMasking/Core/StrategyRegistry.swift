/// Registry mapping strategy names to factory closures.
///
/// Comes pre-loaded with all built-in strategies. Users can register
/// additional custom strategies via `register(name:factory:)`.
public struct StrategyRegistry: Sendable {
    private var factories: [String: @Sendable ([String: String]) -> any MaskingStrategy]

    /// Creates a registry with all built-in strategies pre-registered.
    public init() {
        factories = [:]
        registerBuiltins()
    }

    /// Register a custom masking strategy.
    /// - Parameters:
    ///   - name: The strategy name used in configuration.
    ///   - factory: A closure that creates the strategy from options.
    public mutating func register(
        name: String,
        factory: @escaping @Sendable ([String: String]) -> any MaskingStrategy
    ) {
        factories[name] = factory
    }

    /// Create a strategy instance by name with the given options.
    /// - Throws: `MaskingError.unknownStrategy` if the name is not registered.
    public func create(name: String, options: [String: String] = [:]) throws -> any MaskingStrategy {
        guard let factory = factories[name] else {
            throw MaskingError.unknownStrategy(name)
        }
        return factory(options)
    }

    /// Returns all registered strategy names.
    public var registeredNames: [String] {
        Array(factories.keys).sorted()
    }

    private mutating func registerBuiltins() {
        register(name: NullStrategy.name) { _ in NullStrategy() }
        register(name: RedactStrategy.name) { opts in RedactStrategy(options: opts) }
        register(name: HashStrategy.name) { opts in HashStrategy(options: opts) }
        register(name: NumericNoiseStrategy.name) { opts in NumericNoiseStrategy(options: opts) }
        register(name: PartialStrategy.name) { opts in PartialStrategy(options: opts) }
        register(name: PreserveFormatStrategy.name) { opts in PreserveFormatStrategy(options: opts) }
        register(name: FakeStrategy.name) { opts in FakeStrategy(options: opts) }
        register(name: RegexReplaceStrategy.name) { opts in RegexReplaceStrategy(options: opts) }
    }
}

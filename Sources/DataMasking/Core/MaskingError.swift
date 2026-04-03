/// Errors thrown by the DataMasking library.
public enum MaskingError: Error, Sendable, Equatable {
    case unknownStrategy(String)
    case invalidOption(strategy: String, option: String, reason: String)
    case parserFailed(valueType: String, input: String, detail: String)
    case configLoadFailed(path: String, detail: String)
    case invalidPattern(String)
    case dslParseFailed(expression: String, detail: String)
    case dslEvalFailed(expression: String, detail: String)
}

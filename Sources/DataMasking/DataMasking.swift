// DataMasking — A pure Swift library for data masking and anonymization.
//
// Provides configurable strategies for masking sensitive data fields:
// - hash: Deterministic FNV-1a hashing (preserves FK integrity)
// - fake: Realistic fake data generation
// - redact: Fixed replacement value
// - partial: Partial masking (keep N leading chars)
// - null: Replace with NULL
// - preserve-format: Randomize content while keeping format
// - regex: Regex-based replacement
// - numeric-noise: Add noise to numbers for aggregate obfuscation
//
// Configuration can be built programmatically or loaded from YAML.
// A DSL expression language enables complex per-field transforms.

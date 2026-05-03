# Delta for Java Compatibility

## ADDED Requirements

### Requirement: Java 8 Source Compatibility
The system MUST compile with Java 8 (`source=1.8`, `target=1.8`) without using Java 9+ language or library features.

#### Scenario: Map initialization without Map.of
- GIVEN `TypeMapper.java` defines static type mapping tables
- WHEN the project is compiled with `-source 1.8`
- THEN `Map.of()` MUST NOT be used; `new HashMap<>()` with `put()` calls MUST be used instead

#### Scenario: List initialization without List.of
- GIVEN test files create immutable lists
- WHEN compiled with Java 8
- THEN `List.of()` MUST NOT be used; `java.util.Arrays.asList()` or `new ArrayList<>()` MUST be used instead

#### Scenario: Set initialization without Set.of
- GIVEN test files create immutable sets
- WHEN compiled with Java 8
- THEN `Set.of()` MUST NOT be used; `new HashSet<>()` with `add()` calls MUST be used instead

#### Scenario: No var keyword usage
- GIVEN `SimpleTestRunner.java` and `TypeMapperTest.java` declare local variables
- WHEN compiled with Java 8
- THEN the `var` keyword MUST NOT appear

### Requirement: Explicit Type Declarations
The system MUST use explicit types for all local variable declarations in Java 8 source files.

#### Scenario: Variable types are explicit
- GIVEN any `.java` file in `src/main` or `src/test`
- WHEN searching for `var ` declarations
- THEN zero occurrences MUST be found

## MODIFIED Requirements

## REMOVED Requirements

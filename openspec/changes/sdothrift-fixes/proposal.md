# Proposal: SDO Thrift Data Handler Critical Fixes

## Intent
The SDO Thrift Data Handler project is a custom IBM Integration Designer data handler for bidirectional transformation between Apache Thrift objects and SDO DataObjects. The codebase was delivered with critical build and runtime issues that prevent compilation, testing, and deployment. This change fixes all blocking issues to make the project buildable and the core logic sound.

## Scope

### In Scope
- Fix `pom.xml` structural errors (duplicate tags, missing closings, non-existent plugin versions)
- Remove/resolvable missing dependencies (`org.eclipse.emf.ecore.sdo`, `com.ibm.wbiserver`)
- Replace Java 9+ APIs (`Map.of`, `List.of`, `Set.of`, `var`) with Java 8 compatible code
- Fix compilation errors in `TestDataGenerator` (self-referencing fields)
- Fix typos (`ThiftSDOConfiguration` -> `ThriftSDOConfiguration`)
- Fix `ThriftToSDOTransformer` struct metadata handling (`FieldValueMetaData.structClass` doesn't exist)
- Remove unreachable code and dead logic in `ThriftToSDOTransformer`
- Fix JSON direction detection logic in `ThriftSDODataHandler`
- Ensure the project compiles with `mvn compile` or direct `javac`

### Out of Scope
- Adding new features or new data type support
- Full integration testing with IBM Integration Designer runtime
- Generating actual Thrift compiler output (test structs remain hand-written stubs)
- Performance optimization
- Rewriting the entire transformer architecture

## Approach
1. **Build system repair** — Correct `pom.xml` tags and plugin versions; replace unresolvable dependencies with available equivalents or remove them if runtime-provided.
2. **Java 8 backport** — Replace all `Map.of`, `List.of`, `Set.of`, and `var` with `new HashMap<>`, `new ArrayList<>`, `new HashSet<>`, and explicit types.
3. **Source code bug fixes** — Fix `metaDataMap` self-reference, typos, struct metadata access, and unreachable code.
4. **Validation** — Compile the project and verify zero compilation errors.

## Affected Areas
- `pom.xml`
- `src/main/java/com/sdothrift/transformer/TypeMapper.java`
- `src/main/java/com/sdothrift/transformer/ThriftToSDOTransformer.java`
- `src/main/java/com/sdothrift/transformer/SDOToThriftTransformer.java`
- `src/main/java/com/sdothrift/ThriftSDODataHandler.java`
- `src/test/java/com/sdothrift/util/TestDataGenerator.java`
- `src/test/java/com/sdothrift/util/SimpleTestRunner.java`
- `src/test/java/com/sdothrift/ThriftSDODataHandlerTest.java`
- `src/test/java/com/sdothrift/transformer/TypeMapperTest.java`

## Risks
- **IBM/EMF dependencies still missing from Maven Central**: `commonj.connector.runtime.DataHandler`, `EDataObject`, and `org.eclipse.emf.ecore.sdo` are not available. We will stub or conditionally compile around these.
- **Test stubs incomplete**: `TestThriftStruct.read()` and `write()` are empty, so JSON-based round-trip tests may not exercise actual Thrift serialization. This is acceptable for compilation validation.

## Rollback Plan
All changes are tracked in git. If any fix introduces regressions, revert the specific file to its pre-change state.

## Success Criteria
- `mvn compile` (or equivalent `javac` command) completes with **zero errors**
- No Java 9+ specific APIs remain in source or test files
- `ThriftSDODataHandler` and all transformer classes compile without missing symbol errors
- `TestDataGenerator` compiles without self-reference errors

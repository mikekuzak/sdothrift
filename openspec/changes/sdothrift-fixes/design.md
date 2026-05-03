# Design: SDO Thrift Data Handler Critical Fixes

## Technical Approach
The fix is a compilation-and-correctness pass over the existing codebase. We do not rewrite architecture; we fix the specific defects that prevent the code from building and running.

## Architecture Decisions

### Decision: Java 8 Target Retained
**Choice**: Keep `source=1.8` and `target=1.8` and remove all Java 9+ APIs.
**Alternatives considered**: Bump to Java 11 or 17.
**Rationale**: The project explicitly targets IBM Integration Designer environments that may run on Java 8. Changing the baseline would be a larger, riskier change.

### Decision: Replace Unresolvable EMF SDO Dependency with EMF Core Only
**Choice**: Remove `org.eclipse.emf.ecore.sdo` and `com.ibm.wbiserver` dependencies from `pom.xml`; rely on `org.eclipse.emf.ecore` which is resolvable.
**Alternatives considered**: Add Eclipse repository to POM, manually install jars.
**Rationale**: The SDO-specific `EDataObject` interface can be satisfied by EMF core's `EObject` in the context of compilation. For runtime inside IBM ID, these jars are provided by the container. For build validation, we only need compile-time symbols.

### Decision: Fix Rather Than Rewrite Test Thrift Stubs
**Choice**: Fix the `metaDataMap` self-reference and typos, but leave `read()`/`write()` as stubs.
**Alternatives considered**: Generate real Thrift classes with the Thrift compiler.
**Rationale**: The stubs are sufficient for compilation and unit-test structure. A full Thrift compiler setup is out of scope for a fix-only change.

## Data Flow
No data flow changes. The existing Thrift → JSON → SDO → JSON → Thrift pipeline remains.

## File Changes

| File | Change |
|------|--------|
| `pom.xml` | Fix duplicate tags, add missing `</executions>`, update plugin versions, remove `jacoco` from `<dependencies>`, remove unresolvable SDO/IBM deps |
| `TypeMapper.java` | Replace `Map.of()` with `new HashMap<>()` + `put()` |
| `ThriftToSDOTransformer.java` | Add `instanceof StructMetaData` check; remove unreachable code after `return` in `transformStructToSDO` |
| `ThriftSDODataHandler.java` | Remove or differentiate `isValidSDOJson` |
| `TestDataGenerator.java` | Fix `metaDataMap = metaDataMap` to `metaDataMap = tmpMap`; fix duplicate declarations |
| `SimpleTestRunner.java` | Replace `var` with explicit types; replace `List.of` with `new ArrayList<>()` |
| `ThriftSDODataHandlerTest.java` | Fix `ThiftSDOConfiguration` typo; replace `List.of`/`Map.of` |
| `TypeMapperTest.java` | Replace `Set.of` with `new HashSet<>()`; replace `var` with explicit types |

## Testing Strategy
- Compile `src/main/java` with `javac -source 1.8 -target 1.8` against available dependency classpath
- Verify zero compilation errors
- Compile `src/test/java` against the same classpath plus test dependencies (JUnit, AssertJ, Mockito)

## Migration / Rollout
No migration needed. This is a corrective patch.

## Open Questions
- Can we obtain a real `commonj.connector.runtime.DataHandler` jar for full compilation? If not, the class will compile without the interface but may need a stub for tests.
- Should we add an Eclipse repository to `pom.xml` to resolve `org.eclipse.emf.ecore.sdo` properly, or is removing it acceptable for build validation?

# Tasks: SDO Thrift Data Handler Critical Fixes

## Phase 1: Build System Repair
- [ ] 1.1 Fix duplicate `</systemPropertyVariables>` in `maven-surefire-plugin` config
- [ ] 1.2 Add missing `</executions>` close tag for `maven-surefire-plugin`
- [ ] 1.3 Update `maven-source-plugin` version to `3.3.1`
- [ ] 1.4 Update `maven-javadoc-plugin` version to `3.12.0`
- [ ] 1.5 Remove `jacoco-maven-plugin` from `<dependencies>` (keep only in `<build><plugins>`)
- [ ] 1.6 Remove unresolvable dependencies: `org.eclipse.emf.ecore.sdo`, `com.ibm.wbiserver:datahandler-api`, `com.ibm.wbiserver:datahandler-xml`
- [ ] 1.7 Fix contradictory JaCoCo `<excludes>` / `<includes>` configuration

## Phase 2: Java 8 Compatibility Backport
- [ ] 2.1 Replace `Map.of()` in `TypeMapper.java` with `new HashMap<>()` initialization
- [ ] 2.2 Replace `var` in `SimpleTestRunner.java` with explicit types
- [ ] 2.3 Replace `List.of()` in `ThriftSDODataHandlerTest.java` with `new ArrayList<>()` or `Arrays.asList()`
- [ ] 2.4 Replace `Map.of()` in `ThriftSDODataHandlerTest.java` with `new HashMap<>()`
- [ ] 2.5 Replace `Set.of()` in `TypeMapperTest.java` with `new HashSet<>()`
- [ ] 2.6 Replace `var` in `TypeMapperTest.java` with explicit types

## Phase 3: Source Code Bug Fixes
- [ ] 3.1 Fix `TestDataGenerator.TestThriftStruct` `metaDataMap` self-reference (both occurrences)
- [ ] 3.2 Fix `TestDataGenerator.TestNestedStruct` `metaDataMap` self-reference (both occurrences)
- [ ] 3.3 Fix `ThiftSDOConfiguration` typo to `ThriftSDOConfiguration` in `ThriftSDODataHandlerTest.java`
- [ ] 3.4 Fix `ThriftToSDOTransformer.transformStructToSDO` unreachable code — remove dead lines after `return`
- [ ] 3.5 Fix `ThriftToSDOTransformer` struct metadata access — add `instanceof StructMetaData` check and cast
- [ ] 3.6 Fix `ThriftSDODataHandler.isValidSDOJson` — remove or differentiate from `isValidThriftJson`

## Phase 4: Validation
- [ ] 4.1 Compile `src/main/java` with `javac -source 1.8 -target 1.8` and available dependency classpath
- [ ] 4.2 Compile `src/test/java` with test dependencies classpath
- [ ] 4.3 Verify zero compilation errors in both main and test sources
- [ ] 4.4 Run `mvn compile` if Maven resolution succeeds; otherwise document manual compile steps

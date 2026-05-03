# Delta for Build System

## ADDED Requirements

### Requirement: Valid Maven POM
The system MUST produce a parseable `pom.xml` that Maven can resolve and compile.

#### Scenario: Maven parses pom.xml
- GIVEN the project root contains `pom.xml`
- WHEN `mvn compile` is executed
- THEN Maven MUST successfully parse the POM without `ModelParseException`

### Requirement: Resolvable Plugin Versions
The system MUST reference plugin versions that exist in Maven Central.

#### Scenario: Source and Javadoc plugins resolve
- GIVEN `pom.xml` declares `maven-source-plugin` and `maven-javadoc-plugin`
- WHEN Maven resolves the build plugins
- THEN the versions MUST be available in Maven Central (e.g., `3.3.1` and `3.12.0`)

### Requirement: No Duplicate XML Tags
The system MUST not contain duplicate or unclosed XML tags in `pom.xml`.

#### Scenario: Surefire plugin configuration is well-formed
- GIVEN the `maven-surefire-plugin` configuration block
- WHEN the XML is parsed
- THEN `</systemPropertyVariables>` MUST appear exactly once and `</executions>` MUST close the `<executions>` block

### Requirement: Remove Conflicting Dependency Declarations
The system MUST not declare build plugins as runtime/test dependencies.

#### Scenario: JaCoCo is only a plugin
- GIVEN `jacoco-maven-plugin` is needed for coverage
- WHEN reviewing `pom.xml` dependencies
- THEN `jacoco-maven-plugin` MUST NOT appear in `<dependencies>`; it MAY appear only in `<build><plugins>`

### Requirement: Remove Unresolvable Runtime Dependencies
The system MUST handle unresolvable IBM and Eclipse SDO dependencies gracefully.

#### Scenario: Missing IBM/EMF SDO jars
- GIVEN `org.eclipse.emf.ecore.sdo` and `com.ibm.wbiserver:datahandler-api` are not in Maven Central
- WHEN compiling the project
- THEN the build MUST either provide stubs or exclude these dependencies from resolution failure

## MODIFIED Requirements

## REMOVED Requirements

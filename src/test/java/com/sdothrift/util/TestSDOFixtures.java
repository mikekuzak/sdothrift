package com.sdothrift.util;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.sdo.EDataObject;
import org.eclipse.emf.ecore.sdo.impl.DynamicEDataObjectImpl;
import org.eclipse.emf.ecore.sdo.util.SDOUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Independent, real Ecore/SDO fixtures for transformer and handler tests. */
public final class TestSDOFixtures {
    private static final String PACKAGE_NAME = "sdothrift.test.fixtures";

    private TestSDOFixtures() { }

    public static EDataObject basic() {
        EDataObject root = create("TestThriftStruct");
        required(root, 123, "Test Structure", true, 95.5d);
        optional(root, Arrays.asList("tag1", "tag2", "tag3"),
            mapOf("key1", "value1", "key2", "value2"), "nested_value", "nested_description");
        return root;
    }

    public static EDataObject complex() {
        EDataObject root = create("ComplexThriftStruct");
        required(root, 999, "Complex Structure", true, 100.0d);
        optional(root, Arrays.asList("complex", "nested", "structure"),
            mapOf("complex1", "value1", "complex2", "value2"), "complex", "complex description");
        return root;
    }

    public static EDataObject edge() {
        EDataObject root = create("EdgeCaseThriftStruct");
        required(root, Integer.MAX_VALUE, "Edge Case Test", false, Double.MAX_VALUE);
        root.eSet(feature(root, "tags"), new ArrayList<>());
        root.eSet(feature(root, "properties"), new ArrayList<>());
        return root;
    }

    public static EDataObject specialCharacters(String value) {
        EDataObject root = create("SpecialCharactersThriftStruct");
        required(root, 1, value, true, 99.9d);
        optional(root, Arrays.asList("special", "unicode", "emoji"),
            mapOf("special_key", "special_value"), "unicode 🌍", "description with \"quotes\"");
        return root;
    }

    /** Valid required fields and deliberately absent optional fields. */
    public static EDataObject nullPolicy() {
        EDataObject root = create("NullPolicyThriftStruct");
        required(root, 123, "Test Structure", true, 95.5d);
        return root;
    }

    /** A real schema with exactly one missing required feature value. */
    public static EDataObject invalidMissingRequired() {
        EDataObject root = create("InvalidThriftStruct");
        root.eSet(feature(root, "name"), "Invalid Structure");
        root.eSet(feature(root, "active"), true);
        root.eSet(feature(root, "score"), 12.5d);
        return root;
    }

    /** A real SDO object whose schema exists but whose fields are all unset. */
    public static EDataObject allUnset() {
        return create("UnsetThriftStruct");
    }

    public static EDataObject large() {
        EDataObject root = create("LargeThriftStruct");
        required(root, Integer.MAX_VALUE, "Large Performance Test", true, Double.MAX_VALUE);
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < 1000; i++) tags.add("large-item-" + i);
        List<EDataObject> entries = new ArrayList<>();
        for (int i = 0; i < 500; i++) entries.add(entry(root, "large-key-" + i, "large-value-" + i));
        root.eSet(feature(root, "tags"), tags);
        root.eSet(feature(root, "properties"), entries);
        root.eSet(feature(root, "nested"), nested(root, "large-nested", "large nested description"));
        return root;
    }

    /** Explicitly verifies the SDO model contract for LIST versus SET semantics. */
    public static EDataObject collectionContract() {
        EDataObject root = create("CollectionContract");
        root.eSet(feature(root, "tags"), Arrays.asList("duplicate", "duplicate"));
        root.eSet(feature(root, "uniqueTags"), Arrays.asList("duplicate", "duplicate"));
        return root;
    }

    public static EDataObject create(String className) {
        EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
        ePackage.setName(PACKAGE_NAME);
        ePackage.setNsPrefix("sdothrift-test");
        ePackage.setNsURI("urn:sdothrift:test:" + className + ":" + System.identityHashCode(ePackage));
        ePackage.setEFactoryInstance(new DynamicEDataObjectImpl.FactoryImpl());

        EClass rootClass = eClass(className);
        EClass nestedClass = eClass("Nested");
        EClass entryClass = eClass("PropertiesEntry");
        ePackage.getEClassifiers().add(rootClass);
        ePackage.getEClassifiers().add(nestedClass);
        ePackage.getEClassifiers().add(entryClass);

        attribute(rootClass, "id", EcorePackage.Literals.EINTEGER_OBJECT, 1, true);
        attribute(rootClass, "name", EcorePackage.Literals.ESTRING, 1, true);
        attribute(rootClass, "active", EcorePackage.Literals.EBOOLEAN_OBJECT, 1, true);
        attribute(rootClass, "score", EcorePackage.Literals.EDOUBLE_OBJECT, 1, true);
        attribute(rootClass, "tags", EcorePackage.Literals.ESTRING, -1, false);
        attribute(rootClass, "uniqueTags", EcorePackage.Literals.ESTRING, -1, true);
        reference(rootClass, "properties", entryClass, -1, true, false);
        reference(rootClass, "nested", nestedClass, 1, true, true);
        attribute(nestedClass, "value", EcorePackage.Literals.ESTRING, 1, true);
        attribute(nestedClass, "description", EcorePackage.Literals.ESTRING, 1, true);
        attribute(entryClass, "key", EcorePackage.Literals.ESTRING, 1, true);
        attribute(entryClass, "value", EcorePackage.Literals.ESTRING, 1, true);

        return SDOUtil.create(rootClass);
    }

    private static EClass eClass(String name) {
        EClass result = EcoreFactory.eINSTANCE.createEClass();
        result.setName(name);
        return result;
    }

    private static void attribute(EClass owner, String name, org.eclipse.emf.ecore.EDataType type,
                                  int upperBound, boolean unique) {
        EAttribute attribute = EcoreFactory.eINSTANCE.createEAttribute();
        attribute.setName(name);
        attribute.setEType(type);
        attribute.setLowerBound(0);
        attribute.setUpperBound(upperBound);
        attribute.setUnique(unique);
        attribute.setUnsettable(true);
        owner.getEStructuralFeatures().add(attribute);
    }

    private static void reference(EClass owner, String name, EClass type, int upperBound,
                                  boolean containment, boolean unique) {
        EReference reference = EcoreFactory.eINSTANCE.createEReference();
        reference.setName(name);
        reference.setEType(type);
        reference.setLowerBound(0);
        reference.setUpperBound(upperBound);
        reference.setContainment(containment);
        reference.setUnique(unique);
        reference.setUnsettable(true);
        owner.getEStructuralFeatures().add(reference);
    }

    private static void required(EDataObject root, int id, String name, boolean active, double score) {
        root.eSet(feature(root, "id"), id);
        root.eSet(feature(root, "name"), name);
        root.eSet(feature(root, "active"), active);
        root.eSet(feature(root, "score"), score);
    }

    private static void optional(EDataObject root, List<String> tags, Map<String, String> properties,
                                 String nestedValue, String nestedDescription) {
        root.eSet(feature(root, "tags"), tags);
        List<EDataObject> entries = new ArrayList<>();
        properties.forEach((key, value) -> entries.add(entry(root, key, value)));
        root.eSet(feature(root, "properties"), entries);
        root.eSet(feature(root, "nested"), nested(root, nestedValue, nestedDescription));
    }

    private static EDataObject nested(EDataObject root, String value, String description) {
        EClass type = (EClass) root.eClass().getEPackage().getEClassifier("Nested");
        EDataObject object = SDOUtil.create(type);
        object.eSet(feature(object, "value"), value);
        object.eSet(feature(object, "description"), description);
        return object;
    }

    private static EDataObject entry(EDataObject root, String key, String value) {
        EClass type = (EClass) root.eClass().getEPackage().getEClassifier("PropertiesEntry");
        EDataObject object = SDOUtil.create(type);
        object.eSet(feature(object, "key"), key);
        object.eSet(feature(object, "value"), value);
        return object;
    }

    private static org.eclipse.emf.ecore.EStructuralFeature feature(EDataObject object, String name) {
        org.eclipse.emf.ecore.EStructuralFeature feature = object.eClass().getEStructuralFeature(name);
        if (feature == null) throw new IllegalArgumentException("Missing fixture feature " + name);
        return feature;
    }

    private static Map<String, String> mapOf(String... pairs) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) values.put(pairs[i], pairs[i + 1]);
        return values;
    }
}

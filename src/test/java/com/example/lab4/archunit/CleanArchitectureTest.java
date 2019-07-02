package com.example.lab4.archunit;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

@RunWith(ArchUnitRunner.class) // Remove this line for JUnit 5!!
@AnalyzeClasses(packages = "com.example.lab4")
public class CleanArchitectureTest {
    @ArchTest
    ArchRule noDomainClassShouldDependOnAppOrInfraClasses = noClasses()
            .that()
            .resideInAnyPackage("com.example.lab4.domain..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("com.example.lab4.infra..", "com.example.lab4.app..", "com.example.lab4", "org.apache.flink..");

    @ArchTest
    ArchRule noAppClassShouldDependOnInfraClasses = noClasses()
            .that()
            .resideInAnyPackage("com.example.lab4.app..")
            .should().dependOnClassesThat()
            .resideInAnyPackage("com.example.lab4.infra..", "com.example.lab4", "org.apache.flink..");

    @ArchTest
    ArchRule infraClassesShouldBeAccessOnlyFromInfra = classes()
            .that()
            .resideInAnyPackage("com.example.lab4.infra..")
            .should().onlyBeAccessed()
            .byAnyPackage("com.example.lab4.infra..", "com.example.lab4");

    @ArchTest
    ArchRule appLayerClassesShouldBeAccessOnlyFromAppAndInfra = classes()
            .that()
            .resideInAnyPackage("com.example.lab4.app..")
            .should().onlyBeAccessed()
            .byAnyPackage("com.example.lab4.infra..", "com.example.lab4.app..");

    @ArchTest
    ArchRule cleanArchitectureDependencyRule = layeredArchitecture()
            .layer("domain").definedBy("com.example.lab4.domain..")
            .layer("app").definedBy("com.example.lab4.app..")
            .layer("infra").definedBy("com.example.lab4.infra..", "com.example.lab4", "org.apache.flink..")
            .whereLayer("domain").mayOnlyBeAccessedByLayers("domain",  "app", "infra")
            .whereLayer("app").mayOnlyBeAccessedByLayers("app", "infra")
            .whereLayer("infra").mayOnlyBeAccessedByLayers("infra");
}

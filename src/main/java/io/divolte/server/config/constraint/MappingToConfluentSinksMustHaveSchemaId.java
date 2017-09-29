package io.divolte.server.config.constraint;

import io.divolte.server.config.DivolteConfiguration;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({ TYPE })
@Retention(RUNTIME)
@Constraint(validatedBy=MappingToConfluentSinksMustHaveSchemaId.Validator.class)
@Documented
public @interface MappingToConfluentSinksMustHaveSchemaId {
    String message() default "Mappings used by sinks in Confluent-mode must have their 'confluent_id' attribute set. The following mappings are missing this: ${validatedValue.mappingsToConfluentSinksWithoutSchemaIds()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<MappingToConfluentSinksMustHaveSchemaId, DivolteConfiguration> {
        @Override
        public void initialize(final MappingToConfluentSinksMustHaveSchemaId constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.mappingsToConfluentSinksWithoutSchemaIds().isEmpty();
        }
    }
}

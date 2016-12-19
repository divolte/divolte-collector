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
@Constraint(validatedBy=MappingToConfluentMustHaveSchemaId.Validator.class)
@Documented
public @interface MappingToConfluentMustHaveSchemaId {
    String message() default "Any Confluent sink must have a schema id. The following mappings refer to a Confluent sink, but do not have a schema ID: ${validatedValue.mappingsToConfluentSinksWithoutSchemaIds()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<MappingToConfluentMustHaveSchemaId, DivolteConfiguration> {
        @Override
        public void initialize(final MappingToConfluentMustHaveSchemaId constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.mappingsToConfluentSinksWithoutSchemaIds().isEmpty();
        }
    }
}

package io.divolte.server.config.constraint;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import io.divolte.server.config.DivolteConfiguration;

@Target({ TYPE })
@Retention(RUNTIME)
@Constraint(validatedBy=OneSchemaPerSink.Validator.class)
@Documented
public @interface OneSchemaPerSink {
    String message() default "Any sink can only use one schema. The following sinks have multiple mappings with different schema's linked to them: ${validatedValue.sinksWithMultipleSchemas()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    public static class Validator implements ConstraintValidator<OneSchemaPerSink, DivolteConfiguration> {
        @Override
        public void initialize(final OneSchemaPerSink constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.sinksWithMultipleSchemas().isEmpty();
        }
    }
}

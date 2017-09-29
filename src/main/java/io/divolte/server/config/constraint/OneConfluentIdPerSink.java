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
@Constraint(validatedBy=OneConfluentIdPerSink.Validator.class)
@Documented
public @interface OneConfluentIdPerSink {
    String message() default "Any sink can only use one confluent identifier. The following sinks have multiple mappings with different 'confluent_id' attributes: ${validatedValue.sinksWithMultipleConfluentIds()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<OneConfluentIdPerSink, DivolteConfiguration> {
        @Override
        public void initialize(final OneConfluentIdPerSink constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.sinksWithMultipleConfluentIds().isEmpty();
        }
    }
}

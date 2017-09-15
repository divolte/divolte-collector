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
@Constraint(validatedBy=ConfluentSinksHaveKeyId.Validator.class)
@Documented
public @interface ConfluentSinksHaveKeyId {
    String message() default "These sinks use mode 'confluent' but 'global.kafka.confluent_key_id' is not set: ${validatedValue.sinksMissingGlobalConfiguration()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    class Validator implements ConstraintValidator<ConfluentSinksHaveKeyId, DivolteConfiguration> {
        @Override
        public void initialize(final ConfluentSinksHaveKeyId constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.sinksMissingGlobalConfiguration().isEmpty();
        }
    }
}

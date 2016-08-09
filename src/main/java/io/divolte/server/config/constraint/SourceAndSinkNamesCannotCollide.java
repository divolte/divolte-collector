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
@Constraint(validatedBy=SourceAndSinkNamesCannotCollide.Validator.class)
@Documented
public @interface SourceAndSinkNamesCannotCollide {
    String message() default "Source and sink names cannot collide (must be globally unique). The following names were both used as source and as sink: ${validatedValue.collidingSourceAndSinkNames()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    public static class Validator implements ConstraintValidator<SourceAndSinkNamesCannotCollide, DivolteConfiguration> {
        @Override
        public void initialize(final SourceAndSinkNamesCannotCollide constraintAnnotation) {
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.collidingSourceAndSinkNames().isEmpty();
        }
    }
}

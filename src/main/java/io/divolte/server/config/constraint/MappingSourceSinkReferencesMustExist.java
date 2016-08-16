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
@Constraint(validatedBy = MappingSourceSinkReferencesMustExist.Validator.class)
@Documented
public @interface MappingSourceSinkReferencesMustExist {
    String message() default "The following sources and/or sinks were used in a mapping but never defined: ${validatedValue.missingSourcesSinks()}.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    public static final class Validator implements ConstraintValidator<MappingSourceSinkReferencesMustExist, DivolteConfiguration>{
        @Override
        public void initialize(final MappingSourceSinkReferencesMustExist constraintAnnotation) {
        }

        @Override
        public boolean isValid(final DivolteConfiguration value, final ConstraintValidatorContext context) {
            return value.missingSourcesSinks().isEmpty();
        }
    }
}

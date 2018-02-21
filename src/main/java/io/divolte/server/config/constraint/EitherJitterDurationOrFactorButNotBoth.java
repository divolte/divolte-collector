/*
 * Copyright 2018 GoDataDriven B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.divolte.server.config.constraint;

import io.divolte.server.config.GoogleCloudStorageRetryConfiguration;

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
@Constraint(validatedBy = EitherJitterDurationOrFactorButNotBoth.Validator.class)
@Documented
public @interface EitherJitterDurationOrFactorButNotBoth {
    String message() default "Retry settings may specify a jitter duration or factor, but not both.";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    final class Validator implements ConstraintValidator<EitherJitterDurationOrFactorButNotBoth, GoogleCloudStorageRetryConfiguration>{
        @Override
        public void initialize(final EitherJitterDurationOrFactorButNotBoth constraintAnnotation) {
            // Nothing needed here.
        }

        @Override
        public boolean isValid(final GoogleCloudStorageRetryConfiguration value, final ConstraintValidatorContext context) {
            // Check that both jitter settings are not set.
            return !value.jitterFactor.isPresent() || !value.jitterDelay.isPresent();
        }
    }
}

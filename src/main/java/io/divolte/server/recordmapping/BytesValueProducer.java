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

package io.divolte.server.recordmapping;

import com.google.common.io.BaseEncoding;
import io.divolte.server.recordmapping.DslRecordMapping.PrimitiveValueProducer;
import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer;
import org.apache.avro.Schema;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.Optional;

@ParametersAreNonnullByDefault
class BytesValueProducer extends PrimitiveValueProducer<ByteBuffer> {
    private static final BaseEncoding HEX_LOWER = BaseEncoding.base16().lowerCase();
    private static final BaseEncoding HEX_UPPER = BaseEncoding.base16().upperCase();
    private static final BaseEncoding BASE_64 = BaseEncoding.base64();

    public BytesValueProducer(final String readableName, final FieldSupplier<ByteBuffer> supplier) {
        super(readableName, ByteBuffer.class, supplier, true);
    }

    @Override
    Optional<ValidationError> validateTypes(final Schema.Field target) {
        return DslRecordMapping.validateTrivialUnion(target.schema(),
                                                     x -> x.getType() == Schema.Type.BYTES,
                                                     "only 'bytes' are supported");
    }

    private ValueProducer<String> encodedProducer(final String identifierSuffix, final BaseEncoding encoder) {
        return new PrimitiveValueProducer<>(this.identifier + identifierSuffix,
                                            String.class,
                                            (e, c) -> this.produce(e, c)
                                                          .map(bb -> encoder.encode(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())));
    }

    public ValueProducer<String> toHexLower() {
        return encodedProducer(".toHexLower()", HEX_LOWER);
    }

    public ValueProducer<String> toHexUpper() {
        return encodedProducer(".toHexUpper()", HEX_UPPER);
    }

    public ValueProducer<String> toBase64() {
        return encodedProducer(".toBase64()", BASE_64);
    }
}

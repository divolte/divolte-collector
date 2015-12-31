/*
 * Copyright 2014 GoDataDriven B.V.
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

package io.divolte.groovyscript

import io.divolte.server.recordmapping.DslRecordMapping.ValueProducer
import io.divolte.server.recordmapping.SchemaMappingException

abstract class MappingBase extends Script {
    // The mapping property is set on the scripts binding by the
    // initializing Java code. This is to avoid cyclic dependencies
    // between Java and Groovy code, as that introduces issues with
    // IDE support.

    def mapping(Closure c) {
        c.delegate = mapping
        c.run()
    }

    def map(literal) {
        [
            'onto': { String fieldName -> mapping.map(fieldName, literal) }
        ]
    }

    def map(ValueProducer producer) {
        [
            'onto': { String fieldName -> mapping.map(fieldName, producer) }
        ]
    }

    def map(Closure<ValueProducer> producer) {
        map(producer.call())
    }

    def when(ValueProducer<Boolean> producer) {
        [
            'apply': { Closure closure -> mapping.when(producer, closure) },
            'stop': { mapping.when(producer, { mapping.stop() }) },
            'exit': { mapping.exitWhen(producer) }
        ]
    }

    def when(Closure<ValueProducer<Boolean>> producer) {
        when(producer.call())
    }

    def match(String regex) {
        [
            'against': { ValueProducer<String> producer -> mapping.matcher(producer, regex) }
        ]
    }

    static ValueProducer<Boolean> not(ValueProducer<Boolean> producer) {
        producer.negate()
    }

    final Class<Integer> int32 = Integer.TYPE
    final Class<Long> int64 = Long.TYPE
    final Class<Float> fp32 = Float.TYPE
    final Class<Double> fp64 = Double.TYPE
    final Class<Boolean> bool = Boolean.TYPE
    final Class<URI> uri = URI.class

    def parse(ValueProducer<String> producer) {
        [
            'to': {
                t -> switch(t) {
                    case int32:
                        mapping.toInt(producer)
                        break
                    case int64:
                        mapping.toLong(producer)
                        break
                    case fp32:
                        mapping.toFloat(producer)
                        break
                    case fp64:
                        mapping.toDouble(producer)
                        break
                    case bool:
                        mapping.toBoolean(producer)
                        break
                    case uri:
                        mapping.parseUri(producer)
                        break
                    default:
                        throw new SchemaMappingException("Cannot parse string into type: %s", t)
                }
            }
        ]
    }
}

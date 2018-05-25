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

mapping {
    map firstInSession() onto 'sessionStart'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    // Simple concatenation of two values.
    map join(userAgentString(), partyId()) onto 'stringJoinSimple'

    // Concatenate nothing.
    map join() onto 'stringJoinEmpty'

    // Concatenation with a separator.
    map joiner.on(',').join(userAgentString(), partyId()) onto 'stringJoinSimpleComma'

    // Concatenate a mix of present and missing attributes.
    map joiner.on(',').join(userAgentString(), header("not-present"), sessionId()) onto 'stringJoinSomeMissing'

    // Concatenation with the start and end specified explicitly.
    map joiner.withPrefix('{').withSuffix('}').join(eventId()) onto 'stringJoinBookends'

    // Concatenate a series where everything is missing.
    map joiner.on(',').withPrefix('[').withSuffix(']').join(header("not-present"), header("also-not-present")) onto 'stringJoinAllMissing'

    // Concatenation with some literals.
    map join('{', eventId(), '}') onto 'stringJoinSomeLiteral'

    // Check Groovy's interpolated strings.
    // (Our "Object..." signature doesn't trigger the normal automagic conversion.)
    final String somethingToInterpolate = "banana"
    map join("leftear<$somethingToInterpolate>rightear", eventId()) onto 'stringJoinGroovyLiteral'

    // Check strings from the JSON world. (JSON that can't be mapped to a string is skipped.)
    map join("literal",
             eventId(),
             eventParameters().path('$.foo'),
             eventParameters().path('$.bar'),
             eventParameters().path('$.items[*].name'),
             eventParameters().path('$.items[0].name')) onto 'stringJoinSomeJson'
}

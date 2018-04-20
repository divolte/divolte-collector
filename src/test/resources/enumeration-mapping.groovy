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

    // We have different ways of mapping to test:

    // 1. Just a simple literal.
    // XXX: Not currently valid; haven't decided yet how to map literals.
    map 'DIAMONDS' onto 'suit'

    // 2. A 'simple' provider with a fixed output type.
    def u = parse location() to uri
    // XXX: Not currently valid, but might be eventually.
    def f = parse u.decodedFragment() to enum
    map f onto 'secondSuit'

    // 3. Mapping via the object mapper.
    map eventParameters().path('$.suit') onto 'thirdSuit'
}

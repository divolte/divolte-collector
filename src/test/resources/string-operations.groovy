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

    // Simply concatenation of two values.
    map concat(userAgentString(), partyId()) onto 'stringConcatSimple'

    // Map nothing.
    map concat() onto 'stringConcatEmpty'

    // Map a mix of present and missing attributes.
    map concat(userAgentString(), header("not-present"), sessionId()) onto 'stringConcatSomeMissing'

    // Map a series where everything is missing.
    map concat(header("not-present"), header("also-not-present")) onto 'stringConcatAllMissing'

    // Map a series where everything is missing.
    map concat_ws("-", userAgentString(), header("not-present"), sessionId()) onto 'stringWithSeparatorConcatAllMissing'

    // Map a series where everything is missing.
    map userAgentString().sha3_256() onto 'stringHashedUsingSha256'
}

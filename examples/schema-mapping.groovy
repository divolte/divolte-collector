/*
 * Copyright 2015 GoDataDriven B.V.
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
    // Simple field mappings.
    // For fields that are potentially not set,
    // make sure that the Avro record field is nullable
    map firstInSession() onto 'firstInSession'
    map timestamp() onto 'ts'
    map remoteHost() onto 'remoteHost'

    map referer() onto 'referer'
    map location() onto 'location'
    map viewportPixelWidth() onto 'viewportPixelWidth'
    map viewportPixelHeight() onto 'viewportPixelHeight'
    map screenPixelWidth() onto 'screenPixelWidth'
    map screenPixelHeight() onto 'screenPixelHeight'
    map devicePixelRatio() onto 'devicePixelRatio'
    map partyId() onto 'partyId'
    map sessionId() onto 'sessionId'
    map pageViewId() onto 'pageViewId'

    map userAgentString() onto 'userAgent'
    def ua = userAgent()
    map ua.name() onto 'userAgentName'
    map ua.family() onto 'userAgentFamily'
    map ua.vendor() onto 'userAgentVendor'
    map ua.type() onto 'userAgentType'
    map ua.version() onto 'userAgentVersion'
    map ua.deviceCategory() onto 'userAgentDeviceCategory'
    map ua.osFamily() onto 'userAgentOsFamily'
    map ua.osVersion() onto 'userAgentOsVersion'
    map ua.osVendor() onto 'userAgentOsVendor'

    section {
        // Pagetype detection

        // Extract the location path; we don't care about the domain.
        def locationUri = parse location() to uri
        def locationPath = locationUri.path()

        // Matches the home page
        // e.g. /
        // e.g. /index.html
        def homepageMatcher = match /^\/(?:index\.html)?$/ against locationPath
        when homepageMatcher.matches apply {
            map 'home' onto 'pageType'
            exit()
        }

        // Viewing product details
        // e.g. /products/311381
        def productDetailMatcher = match /^\/product\/([0-9]{6})$/ against locationPath
        when productDetailMatcher.matches apply {
            map 'product_detail' onto 'pageType'
            map productDetailMatcher.group(1) onto 'productId'
            exit()
        }

        // Search results.
        // e.g. /search?q=search+phrase
        when locationPath.equalTo('/search') apply {
            map 'searchResults' onto 'pageType'
            map locationUri.query().value('q') onto 'searchPhrase'
            exit()
        }

        // Viewing basket
        // e.g. /basket
        when locationPath.equalTo('/basket') apply {
            map 'basket' onto 'pageType'
            exit()
        }

        // Checkout funnel
        // e.g. /checkout
        when locationPath.equalTo('/checkout') apply {
            map 'checkout' onto 'pageType'
            exit()
        }

        // Match different levels of taxonomy pages (up to three levels deep)
        // URL layout is: http://www.example.com/shop/section/category/
        // e.g. http://www.example.com/fashion/jeans/regular/
        // (These are last due to ambiguity with the special URLs above.)

        // Category
        // e.g. /fashion/jeans/regular/
        def categoryMatcher = match /^\/([a-z0-9\-]+)\/([a-z0-9\-]+)\/([a-z0-9\-]+)\/$/ against locationPath
        when categoryMatcher.matches() apply {
            map 'category' onto 'pageType'
            map categoryMatcher.group(1) onto 'shop'
            map categoryMatcher.group(2) onto 'section'
            map categoryMatcher.group(3) onto 'category'
            exit()
        }

        // Section
        // e.g. /fashion/jeans/
        def sectionMatcher = match /^\/([a-z0-9\-]+)\/([a-z0-9\-]+)\/$/ against locationPath
        when sectionMatcher.matches() apply {
            map 'section' onto 'pageType'
            map sectionMatcher.group(1) onto 'shop'
            map sectionMatcher.group(2) onto 'section'
            exit()
        }

        // Stop
        // e.g. /fashion/jeans/
        def shopMatcher = match /^\/([a-z0-9\-]+)\/$/ against locationPath
        when shopMatcher.matches() apply {
            map 'section' onto 'pageType'
            map shopMatcher.group(1) onto 'shop'
            exit()
        }
    }
}

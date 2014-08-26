/**
 * Divolte JavaScript Library
 * http://github.com/divolte/divolte
 *
 * @license Copyright 2014 GoDataDriven.
 * Released under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 */
(function (global, factory) {
  factory(global);
}('undefined' !== typeof window ? window : this, function(window) {
  "use strict";
  window.console.log("Initializing DVT.");

  // Find the <script> element used to load this script.
  var dvtElement = function() {
    var document = window.document,
        myElement = document.currentScript;
    if ('undefined' === typeof myElement) {
      myElement = document.getElementById("divolte");
      if (null == myElement ||
          'script' !== myElement.tagName.toLowerCase()) {
        myElement = undefined;
      }
    }
    if ('undefined' === typeof myElement ||
        'undefined' === typeof myElement.id ||
        'divolte' !== myElement.id) {
      throw "DVT not initialized correctly; script element missing id='divolte'.";
    }
    return myElement;
  }();
  // Detect the base URL for the Divolte server that served this file.
  var baseURL = function(element) {
    var myUrl = element.src;
    return myUrl.substr(0, 1 + myUrl.lastIndexOf('/'));
  }(dvtElement);
  window.console.info("Divolte base URL detected", baseURL);
  // Figure out the pageview ID, if one is present.
  var pageViewId = function(element) {
    var myUrl = element.src,
        anchorIndex = myUrl.indexOf("#"),
        anchor = -1 !== anchorIndex ? myUrl.substring(anchorIndex + 1) : undefined;
    if ('undefined' !== typeof anchor) {
      if (-1 !== anchor.indexOf('/')) {
        throw "DVT not initialized correctly; page view ID may not contain a slash ('/').";
      }
      window.console.info("Page view ID: " + anchor);
    } else {
      window.console.log("Page view ID deferred until after first event.");
    }
    return anchor;
  }(dvtElement);

  // Declare a function that can be used to generate a reasonably unique string.
  // The string need not be globally unique, but only for this client.
  var digits = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxzy0123456789~_',
      generateCacheNonce = function() {
        return new Date().getTime().toString(36)
          + digits[Math.floor(Math.random() * digits.length)]
          + digits[Math.floor(Math.random() * digits.length)]
          + digits[Math.floor(Math.random() * digits.length)];
  };

  // Declare our module.
  var dvt = {
    _pageViewId: pageViewId,
    // Basic event logger.
    'signal': function() {
      var documentElement = document.documentElement,
          bodyElement = document.getElementsByName('body')[0],
          event = {
            // property names as strings are required here
            // because minifaction otherwise changes them
            // which breaks the query params contract
            'p': dvt._pageViewId,
            'l': window.location.href,
            'r': document.referrer || undefined,
            'i': window.screen.availWidth,
            'j': window.screen.availHeight,
            'w': window.innerWidth || documentElement.clientWidth || bodyElement.clientWidth,
            'h': window.innerHeight || documentElement.clientHeight || bodyElement.clientHeight
          };

      // Initialize with a special cache-busting parameter.
      // (By making it different for every request, it should never come out of a cache.)
      var params = 'n=' + encodeURIComponent(generateCacheNonce());
      // These are the parameters relating to the event itself.
      for (var name in event) {
        if (event.hasOwnProperty(name)) {
          var value = event[name];
          if (typeof value !== 'undefined') {
            params += '&' + name + '=' + encodeURIComponent(value);
          }
        }
      }
      // Special special cache-busting parameter.
      var image = new Image(1,1);
      image.src = baseURL + 'event?' + params;

      // If we don't have a pageViewId yet, we'll add an onload handler
      // to the pixel request to set it after the initial event signal
      if ('undefined' === typeof dvt._pageViewId) {
        image.onload = function() {
          var cookies = document.cookie ? document.cookie.split('; ') : [];
          for (var i = 0, l = cookies.length; i < l; i++) {
            var parts = cookies[i].split('=');
            if (parts.shift() == '_dvv') {
              var pageViewId = parts.shift();
              dvt['_pageViewId'] = pageViewId;
              window.console.info("Divolte-generated page view ID: " + pageViewId);
              break;
            }
          }
        }
      }
    }
  };

  // Expose dvt and $$$ identifiers.
  if (typeof define === "function" && define.amd) {
    define(function() { return dvt; });
  } else if (typeof module !== 'undefined' && module.exports) {
    module.exports = dvt;
  } else {
    window['$$$'] = window['dvt'] = dvt;
  }
  window.console.log("Module initialized.", dvt);

  window.console.log("Firing initial event.");
  dvt['signal']();

  return dvt;
}));

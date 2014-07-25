/*!
 * Divolte JavaScript Library
 * http://github.com/divolte/divolte
 *
 * Copyright 2014 GoDataDriven.
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
  // Figure out the pageview ID, if one is present.
  var pageViewId = function(element) {
    var myUrl = element.src,
        anchorIndex = myUrl.indexOf("#"),
        anchor = -1 !== anchorIndex ? myUrl.substring(anchorIndex + 1) : undefined;
    if ('undefined' !== typeof anchor && -1 !== anchor.indexOf('/')) {
      throw "DVT not initialized correctly; page view ID may not contain a slash ('/').";
    }
    return anchor;
  }(dvtElement);
  // Detect the base URL for the Divolte server that served this file.
  var baseURL = function(element) {
    var myUrl = element.src;
    return myUrl.substr(0, 1 + myUrl.lastIndexOf('/'));
  }(dvtElement);
  window.console.info("Divolte base URL detected", baseURL);

  // Declare our module.
  var dvt = {
    // Basic event logger.
    signal: function() {
      var documentElement = document.documentElement,
          bodyElement = document.getElementsByName('body')[0],
          event = {
            p: pageViewId,
            l: window.location.href,
            r: document.referrer || undefined,
            i: window.screen.availWidth,
            j: window.screen.availHeight,
            w: window.innerWidth || documentElement.clientWidth || bodyElement.clientWidth,
            h: window.innerHeight || documentElement.clientHeight || bodyElement.clientHeight
          };

      var params = "";
      for (var name in event) {
        if (event.hasOwnProperty(name)) {
          var value = event[name];
          if (typeof value !== 'undefined') {
            params += name + '=' + encodeURIComponent(value) + '&';
          }
        }
      }
      if (0 < params.length) {
        params = params.substring(0, params.length - 1);
      }
      // TODO: Add cache-busting unique parameter.
      new Image(1,1).src = baseURL + 'event?' + params;
    }
  };

  // Expose dvt and $$$ identifiers.
  if (typeof define === "function" && define.amd) {
    define(function() { return dvt; });
  } else if (typeof module !== 'undefined' && module.exports) {
    module.exports = dvt;
  } else {
    window.dvt = window.$$$ = dvt;
  }
  window.console.log("Module initialized", dvt);

  window.console.log("Firing initial event");
  dvt.signal();

  return dvt;
}));

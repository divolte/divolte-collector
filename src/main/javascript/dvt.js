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

  // Set up references that we frequently use.
  var document = window.document,
      navigator = window.navigator,
      documentElement = document.documentElement,
      bodyElement = document.getElementsByName('body')[0],
      console = window.console,
      // On some browsers, logging functions are methods that need to access the console as 'this'.
      bound = function(method, instance) {
        return method.bind ? method.bind(instance) : method;
      },
      log = console ? bound(console.log, console) : function() {},
      info = console ? bound(console.info, console) : function() {},
      warn = console ? bound(console.warn, console) : function() {},
      error = console ? bound(console.error, console) : function() {};

  log("Initializing DVT.");

  // Maximum ages of the party and session identifiers.
  var partyIdentifierMaxAge   = 2 * 365 * 24 * 60 * 60,
      sessionIdentifierMaxAge =                30 * 60;

  // Find the <script> element used to load this script.
  var dvtElement = function() {
    /*
     * Modern browsers set a 'currentScript' attribute to the script element
     * of the running script, so we check that first. If that fails we
     * fall back to searching the document for the <script> tag, identified
     * by the 'divolte' id.
     */
    var document = window.document,
        myElement = document['currentScript'];
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

  // Some current browser features that we send to Divolte.
  var screenWidth = window.screen.availWidth,
      screenHeight = window.screen.availHeight,
      windowWidth = function() {
        return window['innerWidth'] || documentElement['clientWidth'] || bodyElement['clientWidth'] || documentElement['offsetWidth'] || bodyElement['offsetWidth'];
      },
      windowHeight = function() {
        return window['innerHeight'] || documentElement['clientHeight'] || bodyElement['clientHeight'] || documentElement['offsetHeight'] || bodyElement['offsetHeight'];
      };

  // Detect the base URL for the Divolte server that served this file.
  var baseURL = function(element) {
    var myUrl = element.src;
    return myUrl.substr(0, 1 + myUrl.lastIndexOf('/'));
  }(dvtElement);
  info("Divolte base URL detected", baseURL);

  // Some utilities for working with cookies.
  var getCookie = function(name) {
        // Assumes cookie name and value are sensible.
        return document.cookie.replace(new RegExp("(?:(?:^|.*;)\\s*" + name + "\\s*\\=\\s*([^;]*).*$)|^.*$"), "$1") || null;
      },
      setCookie = function(name, value, maxAgeSeconds, nowMs) {
        var expiry = new Date(nowMs + 1000 * maxAgeSeconds);
        // Assumes cookie name and value are sensible. (For our use they are.)
        // Note: No domain means these are always first-party cookies.
        document.cookie = name + '=' + value + "; path=/; expires=" + expiry.toUTCString() + "; max-age=" + maxAgeSeconds;
      };

  // A server-side pageview is placed as the anchor of the Divolte script.
  var getServerPageView = function(element) {
    var myUrl = element.src,
        anchorIndex = myUrl.indexOf("#"),
        anchor = -1 !== anchorIndex ? myUrl.substring(anchorIndex + 1) : undefined;
    if ('undefined' !== typeof anchor && -1 !== anchor.indexOf('/')) {
      throw "DVT not initialized correctly; page view ID may not contain a slash ('/').";
    }
    return anchor;
  };

  // Convenience method for the current time.
  var now = function() {
    // Older IE doesn't support Date.now().
    return new Date().getTime();
  };

  /*
   * Implementation of SHA3 (256).
   *
   * This is based on an original implementation by Chris Drost of drostie.org
   * and placed into the public domain. (Thanks!)
   */
  var sha3_256 = function() {
    var permute = [0, 10, 20, 5, 15, 16, 1, 11, 21, 6, 7, 17, 2, 12, 22, 23, 8, 18, 3, 13, 14, 24, 9, 19, 4],
        RC = [ 0x1, 0x8082, 0x808a, 0x80008000, 0x808b, 0x80000001, 0x80008081, 0x8009,
               0x8a, 0x88, 0x80008009, 0x8000000a, 0x8000808b, 0x8b, 0x8089, 0x8003,
               0x8002, 0x80, 0x800a, 0x8000000a, 0x80008081, 0x8080 ],
        r = [0, 1, 30, 28, 27, 4, 12, 6, 23, 20, 3, 10, 11, 25, 7, 9, 13, 15, 21, 8, 18, 2, 29, 24, 14],
        rotate = function(s, n) {
          return (s << n) | (s >>> (32 - n));
        };
    return function (message) {
      var i,
          state = [];
      for (i = 0; i < 25; i += 1) {
        state[i] = 0;
      }
      if (message.length % 16 === 15) {
        message += "\u8001";
      } else {
        message += "\x01";
        while (message.length % 16 !== 15) {
          message += "\0";
        }
        message += "\u8000";
      }
      for (var b = 0; b < message.length; b += 16) {
        for (i = 0; i < 16; i += 2) {
          state[i / 2] ^= message.charCodeAt(b + i) + message.charCodeAt(b + i + 1) * 65536;
        }
        for (var round = 0; round < 22; round += 1) {
          var C = [];
          for (i = 0; i < 5; i += 1) {
            C[i] = state[i] ^ state[i + 5] ^ state[i + 10] ^ state[i + 15] ^ state[i + 20];
          }
          var D = [];
          for (i = 0; i < 5; i += 1) {
            D[i] = C[(i + 4) % 5] ^ rotate(C[(i + 1) % 5], 1);
          }
          var next = [];
          for (i = 0; i < 25; i += 1) {
            next[permute[i]] = rotate(state[i] ^ D[i % 5], r[i]);
          }
          for (i = 0; i < 5; i += 1) {
            for (var j = 0; j < 25; j += 5) {
              state[j + i] = next[j + i] ^ ((~ next[j + (i + 1) % 5]) & (next[j + (i + 2) % 5]));
            }
          }
          state[0] ^= RC[round];
        }
      }
      var output = [];
      for (i = 0; i < 8; ++i) {
        var n = state[i];
        output.push(n & 255, n >>> 8, n >>> 16, n >>> 24);
      }
      return output;
    }}();

  // A function for generating a unique identifier, optionally prefixed with a timestamp.
  var generateId = function() {
    /*
     * A unique identifier is either:
     *  - Some random data; or
     *  - A hash of:
     *     1) The time;
     *     2) Some features specific to the current browser;
     *     3) Rubbish random values from Math.random().
     *
     * The reason for this complication is that producing globally unique values from
     * within a browser is non-trivial.
     */

    // Detect crypto extensions for generating random data.
    var crypto = window['crypto'] || window['msCrypto'],
        getRandomValues = ('undefined' !== typeof crypto) ? crypto['getRandomValues'] : undefined,
        isKnownRandom = ('undefined' !== typeof getRandomValues),
        genRandom;
    if (isKnownRandom) {
      getRandomValues = getRandomValues.bind(crypto);
      genRandom = function(length) {
        // We have Crypto extensions. Use them directly.
        var array = new Uint8Array(length);
        getRandomValues(array);
        return array;
      }
    } else {
      var math = Math,
          // What we want here is the smallest set that discriminates best amongst users.
          // This list is relatively arbitrary based on existing lists that people use.
          // (It doesn't necessarily meet the criteria of most efficient discrimination.)
          probeMimeTypes = [
            "application/pdf",
            "video/quicktime",
            "video/x-msvideo",
            "audio/x-pn-realaudio-plugin",
            "audio/mpeg3",
            "application/googletalk",
            "application/x-mplayer2",
            "application/x-director",
            "application/x-shockwave-flash",
            "application/x-java-vm",
            "application/x-googlegears",
            "application/x-silverlight"
          ],
          // Poor quality randomness based on Math.random().
          randomValue = function() {
            return math.floor(math.random() * 0x7fffffff).toString(36);
          },
          getMimeTypeInformation = function() {
            var plugins,
                mimeTypes = navigator.mimeTypes;
            if (mimeTypes) {
              plugins = "plugins:";
              for (var i = 0, l = probeMimeTypes.length; i < l; ++i) {
                var probeMimeType = probeMimeTypes[i];
                plugins += probeMimeType in mimeTypes ? '1' : '0';
              }
            } else {
              plugins = "";
            }
            return plugins;
          },
          probeActiveXControls = [
            "ShockwaveFlash.ShockwaveFlash.1",
            "AcroPDF.PDF",
            "AgControl.AgControl",
            "QuickTime.QuickTime"
          ],
          getActiveXTypeInformation = function() {
            var plugins;
            if ('ActiveXObject' in window) {
              plugins = "activex:";
              for (var i = 0, l = probeActiveXControls.length; i < l; ++i) {
                var probeActiveXControl = probeActiveXControls[i];
                try {
                  var plugin = new ActiveXObject(probeActiveXControl);
                  plugins += "1";
                  if ('getVersions' in plugin) {
                    plugins += "(" + plugin['getVersions']() + ")";
                  } else if ('getVariable' in plugin) {
                    plugins += "(" + plugin['getVariable']("$version") + ")";
                  }
                } catch(unused) {
                  plugins += '0';
                }
              }
            } else {
              plugins = "";
            }
            return plugins;
          };
      genRandom = function(length, ts) {
        // Build up the data to mix into the hash.
        var winWidth = windowWidth(),
            winHeight = windowHeight(),
            message = [
              // Number of milliseconds since 1970.
              ts.toString(36),
              // Some browser features that should vary between users.
              navigator['userAgent'] || "",
              navigator['platform'] || "",
              navigator['language'] || "",
              navigator['systemLanguage'] || "",
              navigator['userLanguage'] || "",
              screenWidth ? screenWidth.toString(36) : '',
              screenHeight ? screenHeight.toString(36) : '',
              winWidth ? winWidth.toString(36) : '',
              winHeight ? winWidth.toString(36) : '',
              // A mask that depends on some plugin-supported MIME types.
              getMimeTypeInformation(),
              getActiveXTypeInformation(),
              // Some random numbers. These are poor quality.
              randomValue(),
              randomValue(),
              randomValue(),
              randomValue()
            ];
        // There is no point trying to use a secure hash here: the entropy is simply too low.
        var digest = sha3_256(message.join(""));
        if (digest.length != length) {
          throw "Length mismatch.";
        }
        return digest;
      }
    }

    // Digits we use for encoding. Length is 64 to allow encoding 6 bits.
    var digits = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxzy0123456789~_',
        // The maximum number of bytes that can be trivially generated by our hash-based
        // fall-back.
        identifierLength = 32,
        generateDigits = function(ts) {
          var randomData = genRandom(identifierLength, ts);
          var id = "";
          for (var i = 0, l = randomData.length; i < l; ++i) {
            // Warning: IE6 doesn't support [] syntax on strings.
            id += digits.charAt(randomData[i] & 0x3f);
          }
          if (!isKnownRandom) {
            id += '!';
          }
          return id;
        };

    // Time of module initialization. (Used to ensure consistent timestamps.)
    var scriptLoadTime = now();

    return function(includeTimestampPrefix) {
      var digits = generateDigits(scriptLoadTime);
      // For now our identifiers are version 0.
      return "0:" + (includeTimestampPrefix ? (scriptLoadTime.toString(36) + ':' + digits) : digits);
    };
  }();

  // Locate our identifiers, or generate them if necessary.
  var partyId    = getCookie("_dvp"),
      sessionId  = getCookie("_dvs"),
      pageViewId = getServerPageView(dvtElement),
      isNewParty = !partyId,
      isFirstInSession = !sessionId,
      isServerPageView = Boolean(pageViewId);
  if (isNewParty) {
    log("New party; generating identifier.");
    partyId = generateId(true);
  }
  if (isFirstInSession) {
    log("New session; generating identifier.");
    sessionId = generateId(true);
  }
  if (isServerPageView) {
    log("Using server-provided pageview identifier.")
  } else {
    pageViewId = generateId(false)
  }

  info("Divolte party/session/pageview identifiers", [partyId, sessionId, pageViewId]);

  // Declare the namespace our module will export, with some basic content.
  var dvt = {
    'partyId':          partyId,
    'sessionId':        sessionId,
    'pageViewId':       pageViewId,
    'isNewPartyId':     isNewParty,
    'isFirstInSession': isFirstInSession,
    'isServerPageView': isServerPageView
  };

  // A function for generating an event ID. Each time we invoke this, it will
  // return a new identifier.
  var generateEventId = function() {
    // These don't have to be globally unique. So we can leverage the pageview
    // id with a simple counter.
    var counter = 0;
    return function() {
      var thisEventCounter = counter++;
      return pageViewId + thisEventCounter.toString(16);
    }
  }();

  /**
   * Event logger.
   *
   * Invoking this method will cause an event to be logged with the Divolte
   * server. This function returns immediately, the event itself is logged
   * asynchronously.
   *
   * @param {!string} type The type of event to log.
   * @param {object=} [customParameters] Optional object containing custom parameters to log alongside the event.
   *
   * @return {string} the unique event identifier for this event.
   */
  var signal = function(type, customParameters) {
    // Only proceed if we have an event type.
    var eventId;
    if (type) {
      eventId = generateEventId();
      if ('undefined' === typeof customParameters) {
        info("Signalling event: " + type, eventId);
      } else {
        info("Signalling event: " + type, eventId, customParameters);
      }
      var referrer = document.referrer,
          eventTime = now(),
          event = {
            // Note: numbers will be automatically base-36 encoded.
            'p': partyId,
            's': sessionId,
            'v': pageViewId,
            'e': eventId,
            'c': eventTime,
            'n': isNewParty ? 't' : 'f',
            'f': isFirstInSession ? 't' : 'f',
            'l': window.location.href,
            'r': referrer ? referrer : undefined,
            'i': screenWidth,
            'j': screenHeight,
            'k': window['devicePixelRatio'],
            'w': windowWidth(),
            'h': windowHeight(),
            't': type
          };

      // We don't need anything special for cache-busting; the event ID ensures that each
      // request is for a new and unique URL.

      var params = "",
          addParam = function(name,value) {
            if (params.length > 0) {
              params += '&';
            }
            // Value can safely contain '&' and '=' without any problems.
            params += name + '=' + encodeURIComponent(value);
          };

      // These are the parameters relating to the event itself.
      for (var name in event) {
        if (event.hasOwnProperty(name)) {
          var value = event[name];
          switch (typeof value) {
            case 'undefined':
              // No value available. Omit parameter entirely.
              break;
            case 'number':
              // Base 36 encoding for numbers.
              addParam(name, value.toString(36));
              break;
            default:
              addParam(name, value);
          }
        }
      }
      // These are the custom parameters that may have been supplied.
      switch (typeof customParameters) {
        case 'undefined':
          // No custom parameters were supplied.
          break;
        case 'object':
          for (var customName in customParameters) {
            if (customParameters.hasOwnProperty(customName)) {
              var customParameter = customParameters[customName];
              switch (typeof customParameter) {
                case 'string':
                case 'number':
                case 'boolean':
                  addParam('t.' + customName, customParameter);
              }
            }
          }
          break;
        default:
          error("Ignoring non-object custom event parameters", customParameters);
      }

      // After the first request it's neither a new party nor a new session,
      // as far as events are concerned.
      // (We don't modify the module exports: they refer to the page view.)
      isNewParty = false;
      isFirstInSession = false;

      // Update the party and session cookies.
      setCookie("_dvs", sessionId, sessionIdentifierMaxAge, eventTime);
      setCookie("_dvp", partyId, partyIdentifierMaxAge, eventTime);

      var image = new Image(1,1);
      image.src = baseURL + 'csc-event?' + params;
    } else {
      warn("Ignoring event with no type.");
      eventId = undefined;
    }
    return eventId;
  };
  dvt['signal'] = signal;

  // Expose dvt and $$$ identifiers.
  if (typeof define === "function" && define['amd']) {
    define(function() { return dvt; });
  } else if (typeof module !== 'undefined' && module['exports']) {
    module['exports'] = dvt;
  } else {
    window['$$$'] = window['dvt'] = dvt;
  }
  log("Module initialized.", dvt);

  // On load we always signal the 'pageView' event.
  signal('pageView');

  return dvt;
}));

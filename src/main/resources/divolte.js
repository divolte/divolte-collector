/**
 * Divolte JavaScript Library
 * http://github.com/divolte/divolte
 *
 * @license Copyright 2014 GoDataDriven.
 * Released under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 */

// Declared as global, the Closure compiler will inline these.
// (The closure compiler requires them to be declared globally.)

/** @define {string} */
var PARTY_COOKIE_NAME = '_dvp';
/** @define {number} */
var PARTY_ID_TIMEOUT_SECONDS = 2 * 365 * 24 * 60 * 60;
/** @define {string} */
var SESSION_COOKIE_NAME = '_dvs';
/** @define {number} */
var SESSION_ID_TIMEOUT_SECONDS = 30 * 60;
/** @define {string} */
var COOKIE_DOMAIN = '';
/** @define {boolean} */
var LOGGING = false;
/** @define {string} */
var SCRIPT_NAME = 'divolte.js';

(function (global, factory) {
  factory(global);
}('undefined' !== typeof window ? window : this, function(window) {
  "use strict";

  // Alias some references that we frequently use.
  var document = window.document,
      navigator = window.navigator,
      documentElement = document.documentElement,
      bodyElement = document.getElementsByName('body')[0],
      console = window.console,
      // On some browsers, logging functions are methods that expect to access the console as 'this'.
      bound = function(method, instance) {
        return method.bind ? method.bind(instance) : method;
      },
      log = LOGGING && console ? bound(console.log, console) : function() {},
      info = LOGGING && console ? bound(console.info, console) : function() {},
      warn = LOGGING && console ? bound(console.warn, console) : function() {},
      error = LOGGING && console ? bound(console.error, console) : function() {};

  log("Initializing Divolte.");

  /**
   * The URL used to load this script.
   * (This will include the anchor on the URL, if any.)
   *
   * @const
   * @type {string}
   */
  var divolteScriptUrl = function() {
    var couldNotInitialize = function(reason) {
      var error = "Divolte could not initialize itself";
      if (LOGGING) {
        error += '; ' + reason;
      }
      error += '.';
      throw error;
    };
    /*
     * Modern browsers set a 'currentScript' attribute to the script element
     * of the running script, so we check that first. If that fails we fall
     * back to searching the document for a <script> tag that refers to the
     * our script name, as configured by the globally defined SCRIPT_NAME
     * value.
     */
    var myElement = document['currentScript'];
    var url;
    if ('undefined' === typeof myElement) {
      var regexEscape = function (s) {
        return s.replace(/([.*+?^${}()|\[\]\/\\])/g, "\\$1");
      };
      var scriptElements = document.getElementsByTagName('script');
      var scriptPattern = new RegExp("^(:?.*\/)?" + regexEscape(SCRIPT_NAME) + "(:?[?#].*)?$");
      for (var i = scriptElements.length - 1; i >= 0; --i) {
        var scriptElement = scriptElements.item(i);
        var scriptUrl = scriptElement.src;
        if (scriptPattern.test(scriptUrl)) {
          if ('undefined' == typeof url) {
            url = scriptUrl;
          } else {
            couldNotInitialize('multiple <script> elements found with src="…/' + SCRIPT_NAME + '"');
          }
        }
      }
    } else {
      url = myElement.src;
    }
    if ('undefined' === typeof url) {
      couldNotInitialize('could not locate <script> with src=".../' + SCRIPT_NAME + '"');
    }
    return url;
  }();

  /**
   * The base URL for the Divolte server that served this file.
   *
   * @const
   * @type {string}
   */
  var divolteUrl = function(myUrl) {
    return myUrl.substr(0, 1 + myUrl.lastIndexOf('/'));
  }(divolteScriptUrl);
  info("Divolte base URL detected", divolteUrl);

  /* Some current browser features that we send to Divolte. */
  var
      /**
       * The width of the user screen.
       * @const
       * @type {number}
       */
      screenWidth = window.screen.availWidth,
      /**
       * The height of the user screen.
       * @const
       * @type {number}
       */
      screenHeight = window.screen.availHeight,
      /**
       * Query the current width of the browser window.
       * @return {?number}
       */
      windowWidth = function() {
        return window['innerWidth'] || documentElement['clientWidth'] || bodyElement['clientWidth'] || documentElement['offsetWidth'] || bodyElement['offsetWidth'];
      },
      /**
       * Query the current height of the browser window.
       * @return {?number}
       */
      windowHeight = function() {
        return window['innerHeight'] || documentElement['clientHeight'] || bodyElement['clientHeight'] || documentElement['offsetHeight'] || bodyElement['offsetHeight'];
      };

  /**
   * Get the value of a cookie.
   *
   * @param {string} name   The name of the cookie to retrieve.
   * @return {?string}      the value of the cookie, if the cookie exists, or null otherwise.
   */
  var getCookie = function(name) {
        // Assumes cookie name and value are sensible.
        return document.cookie.replace(new RegExp("(?:(?:^|.*;)\\s*" + name + "\\s*\\=\\s*([^;]*).*$)|^.*$"), "$1") || null;
      };
  /**
   * Set a cookie.
   *
   * @param {string} name          The name of the cookie to set.
   * @param {string} value         The value to assign to the cookie.
   * @param {number} maxAgeSeconds The expiry (age) of the cookie, in seconds from now.
   * @param {number} nowMs         The current time, in milliseconds since the Unix epoch.
   * @param {string} domain        The domain to set the cookies for, if non-zero in length.
   */
  var setCookie = function(name, value, maxAgeSeconds, nowMs, domain) {
        var expiry = new Date(nowMs + 1000 * maxAgeSeconds);
        // Assumes cookie name and value are sensible. (For our use they are.)
        // Note: No domain means these are always first-party cookies.
        var cookieString = name + '=' + value + "; path=/; expires=" + expiry.toUTCString() + "; max-age=" + maxAgeSeconds;
        if (domain) {
          cookieString += "; domain=" + domain;
        }
        document.cookie = cookieString;
      };

  /**
   * Get the server-supplied pageview ID, if present.
   * The server can supply a pageview ID by placing it in the anchor of the script URL.
   *
   * @param {string} myUrl The URL used to load this script.
   * @return {?string} the server-supplied pageview ID, if present, or null if not.
   * @throws {string} if the pageview ID is supplied but contains a slath ('/').
   */
  var getServerPageView = function(myUrl) {
    var anchorIndex = myUrl.indexOf("#"),
        anchor = -1 !== anchorIndex ? myUrl.substring(anchorIndex + 1) : undefined;
    if ('undefined' !== typeof anchor && -1 !== anchor.indexOf('/')) {
      throw "DVT not initialized correctly; page view ID may not contain a slash ('/').";
    }
    return anchor;
  };

  /**
   * Convenience method for the current time.
   * The time is returned as a Java-style timestamp.
   *
   * @return {number} the number of milliseconds since the start of 1970, UTC.
   */
  var now = function() {
    // Older IE doesn't support Date.now().
    return new Date().getTime();
  };

  /**
   * Implementation of SHA3 (256).
   *
   * This is based on an original implementation by Chris Drost of drostie.org
   * and placed into the public domain. (Thanks!)
   *
   * @param {string} message    The message to product a digest of.
   * @return {!Array.<number>} the calculated 256-bit SHA-3 digest of the supplied message.
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

  /**
   * Generate a unique identifier, optionally prefixed with a timestamp.
   * There are two internal implementations, depending on whether Crypto
   * extensions are detected or not.
   *
   * @param {boolean} includeTimestampPrefix  whether or not the a timestamp
   *        should be included in the identifier. (This timestamp is always
   *        the module load time.)
   * @return {string} a unique identifier.
   */
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
        isKnownRandom = ('undefined' !== typeof crypto && 'undefined' !== typeof crypto['getRandomValues']),
        genRandom;
    if (isKnownRandom) {
      genRandom = function(length) {
        // We have Crypto extensions. Use them directly.
        var array = new Uint8Array(length);
        crypto['getRandomValues'](array);
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

    var
        /**
         * Digits used for base-64 encoding.
         * @const
         * @type {string}
         */
        digits = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxzy0123456789~_',
        /**
         * The length of the identifiers that we generate.
         * This corresponds to the maximum number of bytes that can be trivially generated
         * using our hash-based fallback mechanism.
         * @const
         * @type {number}
         */
        identifierLength = 32,
        /**
         * Generate a random identifier.
         * @param {number} ts   a timestamp to use for entropy, if necessary.
         * @return {string} a random identifier.
         */
        generateDigits = function(ts) {
          var randomData = genRandom(identifierLength, ts);
          var id = "";
          for (var i = 0, l = randomData.length; i < l; ++i) {
            // Warning: IE6 doesn't support [] syntax on strings.
            id += digits.charAt(randomData[i] & 0x3f);
          }
          // Mark pseudo-random identifiers with a '!' suffix, so we
          // can see them amongst secure-random identifiers.
          if (!isKnownRandom) {
            id += '!';
          }
          return id;
        };

    /**
     * Time of module initialization.
     * This is used to ensure consistent timestamps when multiple identifiers are generated.
     * @const
     * @type {number}
     */
    var scriptLoadTime = now();

    return function(includeTimestampPrefix) {
      var digits = generateDigits(scriptLoadTime);
      // For now our identifiers are version 0.
      return "0:" + (includeTimestampPrefix ? (scriptLoadTime.toString(36) + ':' + digits) : digits);
    };
  }();

  // Locate our identifiers, or generate them if necessary.
  var partyId    = getCookie(PARTY_COOKIE_NAME),
      sessionId  = getCookie(SESSION_COOKIE_NAME),
      pageViewId = getServerPageView(divolteScriptUrl),
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

  /**
   * Generate an event identifier.
   * Note that the implementation requires that pageview identifiers also be unique.
   *
   * @return {string} a unique event identifier.
   */
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
   * A signal queue.
   * This can hold a list of the signal events that are pending.
   * If a signal event is currently underway, it is always the
   * first element in the queue.
   *
   * @constructor
   * @final
   */
  var SignalQueue = function() {
    /**
     * The internal queue of signal events.
     * @private
     * @const
     * @type {!Array.<string>}
     */
    this.queue = [];
  };
  /**
   * Enqueue a signal event.
   * If none are underway, this will commence. Otherwise it will be queued.
   * @param event {string} the pre-calculated (and rendered) event to queue.
   */
  SignalQueue.prototype.enqueue = function(event) {
    var pendingEvents = this.queue;
    pendingEvents.push(event);
    if (1 == pendingEvents.length) {
      this.deliverFirstPendingEvent();
    }
  };
  /**
   * @private
   * Start the next signal event.
   */
  SignalQueue.prototype.deliverFirstPendingEvent = function() {
    var signalQueue = this;
    var image = new Image(1,1);
    image.onload = function() {
      // Delete this signal from the array.
      var pendingEvents = signalQueue.queue;
      pendingEvents.shift();
      // If there are still pending events, schedule the next.
      if (0 < pendingEvents.length) {
        signalQueue.deliverFirstPendingEvent();
      }
    };
    image.src = divolteUrl + 'csc-event?' + this.queue[0];
  };

  /**
   * The queue for pending signal events.
   * This ensures that signals are received in the same order that
   * they are issued in the browser.
   * @const
   * @type {SignalQueue}
   */
  var signalQueue = new SignalQueue();

  /**
   * Event logger.
   *
   * Invoking this method will cause an event to be logged with the Divolte
   * server. This function returns immediately, the event itself is logged
   * asynchronously.
   *
   * @param {!string} type The type of event to log.
   * @param {Object=} [customParameters] Optional object containing custom parameters to log alongside the event.
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
      setCookie(SESSION_COOKIE_NAME, sessionId, SESSION_ID_TIMEOUT_SECONDS, eventTime, COOKIE_DOMAIN);
      setCookie(PARTY_COOKIE_NAME, partyId, PARTY_ID_TIMEOUT_SECONDS, eventTime, COOKIE_DOMAIN);

      signalQueue.enqueue(params);
    } else {
      warn("Ignoring event with no type.");
      eventId = undefined;
    }
    return eventId;
  };

  /**
   * The namespace that we export.
   * @const
   * @type {{partyId: string,
   *         sessionId: string,
   *         pageViewId: string,
   *         isNewPartyId: boolean,
   *         isFirstInSession: boolean,
   *         isServerPageView: boolean,
   *         signal: function(!string,Object=): string}}
   */
  var divolte = {
    'partyId':          partyId,
    'sessionId':        sessionId,
    'pageViewId':       pageViewId,
    'isNewPartyId':     isNewParty,
    'isFirstInSession': isFirstInSession,
    'isServerPageView': isServerPageView,
    'signal':           signal
  };

  if ("object" !== typeof window['divolte']) {
    // Expose divolte module.
    if (typeof define === "function" && define['amd']) {
      define(function () {
        return divolte;
      });
    } else if (typeof module !== 'undefined' && module['exports']) {
      module['exports'] = divolte;
    } else {
      window['divolte'] = divolte;
    }
    log("Module initialized.", divolte);

    // On load we always signal the 'pageView' event.
    signal('pageView');
  } else {
    warn("Divolte module already initialized; existing module left intact.");
  }

  return divolte;
}));

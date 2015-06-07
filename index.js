/*
  The factlog is a set of dbs consisting of ordered facts.
  A state is a value integrated from all the facts in a db.
  When a fact is recorded, it is also integrated, which creates a new state.
  New facts are rejected unless they indicate the latest age of the db.
 
  For any given db with n facts, the server accepts queries for:
    (1) n and the latest state;
    (2) all facts since fact i (if i <= n);
    (3) fact number i (if i <= n);
    (4) the next after fact number n (blocking).

  For an example db named foo, having 4 facts, these correspond to:
    (1) GET /foo           => { age: 4, state: ... }
    (2) GET /foo/1...      => [A, B, C, D]
    (3) GET /foo/4         => D
    (3) GET /foo/5         => (blocks...) E

  Record a fact like this:
    PUT /foo/5         => 200            if nobody recorded 5 first;
                          409 "Conflict" otherwise.
*/

setTimeout(startHttpServer, 0)

var ages = {}
var states = {}
var blocked = {}

function getAge (db) { return ages[db] }
function getState (db) { return states[db] }
function hasDb (db) { return ages[db] >= 0 }

/* Redis backend */

var redis = new (require("ioredis"))(process.env["REDIS_URL"])

function lookupAgeOfDb (db, callback) {
  redis.llen(db, callback)
}

function recordFact (db, fact, callback) {
  redis.rpush(db, fact, callback)
}

function recallFact (db, age, callback) {
  redis.lindex(db, age - 1, callback)
}

function recallFactsFrom (db, from, callback) {
  redis.lrange(db, from - 1, -1, callback)
}

function recallAllFacts (db, callback) {
  recallFactsFrom(db, 1, callback)
}

/* Integrating facts to in-memory state */

function ensureAgeKnown (db, callback) {
  if (hasDb(db)) callback(null, getAge(db))
  else lookupAgeOfDb(db, callback)
}

function integrateFact (db, fact) {
  console.log("Integrating", fact, "to", db)
  if (fact == "cool")
    return function (state) {
      state.cool = (state.cool || 0) + 1
    }
  else
    throw new Error("impossible")
}

function integrateOldFacts (db, facts) {
  console.log("Integrating", facts.length, "old facts to", db)
  setAge(db, facts.length)
  facts.forEach(integrateOldFact.bind(null, db))
  return states[db]
}

function integrateOldFact (db, fact) {
  var finalize = integrateFact(db, fact)
  if (finalize)
    callFinalizer(finalize, db)
  else
    throw new Error("unrecognized fact")
}

function ensureDbLoaded (db, callback) {
  if (states[db]) callback()
  else recallAllFacts(db, function (error, facts) {
    if (error)
      callback(error)
    else
      try {
        if (facts.length)
          states[db] = {}
        callback(null, integrateOldFacts(db, facts))
      } catch (e) {
        callback(e)
      }
  })
}

/* Recording facts */

function recordFactIfValid (response, db, age, fact) {
  console.log("Trying to record fact", db, age, fact)
  ensureDbLoaded(db, function (error) {
    if (error)
      fail(response, 500, "failed to load db")
    else
      if (age == 1 + getAge(db))
        try {
          var finalize = integrateFact(db, fact)
          finalizeFact(response, db, fact, finalize)
        } catch (e) {
          fail(response, 400, e.message)
        }
    else
      fail(response, 409, "wrong")
  })
}

function callFinalizer (finalize, db) {
  finalize.call(null, states[db])
}

function finalizeFact (response, db, fact, finalize) {
  console.log("Finalizing", fact, "to", db)
  incrementAge(db) // block others
   recordFact(db, fact, function (error, count) {
     if (error) {
       decrementAge(db)
       disaster(response, 500, error.message)
     } else if (count != getAge(db)) {
       decrementAge(db)
       disaster(response, 500, "impossible state")
     } else {
       callFinalizer(finalize, db)
       response.end()
       notifyFact(db, fact)
     }
   })
 }

 function notifyFact (db, fact) {
   if (blocked[db]) {
     blocked[db].forEach(function (waiter) { waiter(fact) })
     blocked[db] = []
   }
 }

 function setAge (db, age) {
   console.log("Setting db age for", db, "to", age)
   ages[db] = age
 }

 function incrementAge (db) {
   setAge(db, getAge(db) + 1)
 }

 function decrementAge (db) {
   setAge(db, getAge(db) - 1)
 }

 /* Serving */

function startHttpServer () {
  console.log("Starting HTTP server on", process.env["FACTLOG_PORT"])
  require('http').createServer(
    function (request, response) {
      console.log("Handling request")
      if (request.method == "GET")
        get(request, response)
      else if (request.method == "PUT")
        put(request, response)
      else
        fail(response, 405, "invalid method")
    }
  ).listen(process.env["FACTLOG_PORT"])
}

function fail (response, code, text) {
  console.log("Failed request:", code, text)
  response.statusCode = response
  response.statusText = text || "Nope."
  response.end((text || "Nope.") + "\n")
}

function disaster (response, code, text) {
  fail(response, code, text)
  throw new Error(text)
}

function get (request, response) {
  console.log("Handling GET " + request.url)
  if (request.url.match(/^\/([^\/]+)(\/.*)?/))
    return getDb(request, response, RegExp.$1, RegExp.$2 || "")
  else
    fail(response, 404, "don't know")
}

function getDb (request, response, db, path) {
  if (path == "")
    getDbState(response, db)
  else if (path.match(/^\/(\d+)\.\.$/))
    sendDbFactsSince(response, db, +RegExp.$1)
  else if (path == '/' + (1 + getAge(db)))
    waitForDb(response, db)
  else if (path.match(/^\/(\d+)$/))
    sendDbFact(response, db, RegExp.$1)
  else
    fail(response, 404, "no such route for db " + db)
}

function getDbState (response, db) {
  console.log("Sending", db, "state")
  ensureDbLoaded(db, function (error) {
    if (error)
      fail(response, 500, "failed to load db")
    else
      json(response, {
        age: getAge(db),
        state: getState(db) || {}
      })
  })
}

function sendDbFact (response, db, age) {
  if (age == 0)
    fail(response, 400, "impossible")
  else ensureAgeKnown(db, function (error, dbAge) {
    if (error)
      fail(response, 500, "couldn't load db")
    else if (age == dbAge + 1)
      waitForDb(response, db)
    else if (age > dbAge + 1)
      fail(response, 404, "fact hasn't happened yet " + dbAge)
    else recallFact(db, age, function (error, result) {
      if (error)
        fail(response, 500, "cannot recall facts")
      else
        text(response, result)
    })
  })
}

function sendDbFactsSince (response, db, age) {
  console.log("Getting facts for", db, "since", age)
  ensureAgeKnown(db, function (error, dbAge) {
    if (error)
      fail(response, 500, "failed to load db")
    if (age > dbAge + 1)
      fail(response, 404, "no such fact yet")
    else if (age == dbAge)
      json(response, [])
    else if (age > 0)
      recallFactsFrom(db, age, function (error, result) {
        if (error)
          fail(response, 500, error.message)
        else
          json(response, result)
      })
    else
      fail(response, 404, "try 1..")
  })
}

function waitForDb (response, db) {
  console.log("Waiting for", db)
  if (!blocked[db]) blocked[db] = []
  blocked[db].push(function (fact) {
    text(response, fact)
  }) // TODO: timeout
}

function put (request, response) {
  console.log("Handling PUT" + request.url)
  if (request.url.match(/^\/([^\/]+)\/(\d+)$/))
    slurp(request, function (fact) {
      recordFactIfValid(response, RegExp.$1, +RegExp.$2, fact)
    })
  else
    fail(response, 404)
}

function json (response, result) {
  response.setHeader("Content-Type", "application/json")
  response.end(JSON.stringify(result) + "\n")
}

function text (response, result) {
  response.setHeader("Content-Type", "text/plain")
  response.end(result)
}

function slurp (request, callback) {
  var chunks = []
  request.on('data', chunks.push.bind(chunks))
  request.on('end', function () {
    callback(Buffer.concat(chunks).toString())
  })
}


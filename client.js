var fetch
if (typeof window != "undefined")
  fetch = window.fetch
else {
  fetch = require('node-fetch')
  fetch.Promise = require('q').Promise
}

function fetchJSON (url, options, callback, errback) {
  return fetch(url, options).then(function (x) {
    return x.json()
  }).then(callback, errback)
}

function fetchText (url, options, callback, errback) {
  return fetch(url, options).then(function (x) {
    return x.text()
  }).then(callback, errback)
}

function Factlog (url, age, state, integrate) {
  this.url = url
  this.age = age || 0
  this.state = state || null
  this.integrate = integrate
}

Factlog.prototype.start = function (errback) {
  errback = errback || this.onError.bind(this)
  var keepGoing = this.integrateFuture.bind(this, errback)
  if (this.state == null)
    this.fetchState(keepGoing, errback)
  else
    this.catchUp(keepGoing, errback)
}

Factlog.prototype.fetchState = function (callback, errback) {
  fetchJSON(this.url, {}, function (result) {
    this.age = result.age
    this.state = result.state
    console.log("Fetched state", this.state)
    callback(this.state)
  }.bind(this), errback)
}

Factlog.prototype.catchUp = function (callback, errback) {
  console.log("Catching up from", this.age + 1)
  fetchJSON(this.url + "/" + (this.age + 1) + "..", {}, function (result) {
    console.log("Integrating", result.length, "facts")
    result.forEach(this.doIntegrate.bind(this))
    callback()
  }.bind(this), errback)
}

Factlog.prototype.doIntegrate = function (fact) {
  this.age++
  this.integrate.call(this.state, fact)
}

Factlog.prototype.integrateNext = function (callback, errback) {
  fetchText(this.url + "/" + (this.age + 1), {}, function (result) {
    this.doIntegrate(result)
    callback(result)
  }.bind(this), errback)
}

Factlog.prototype.integrateFuture = function (errback) {
  this.integrateNext(this.integrateFuture.bind(this), errback)
}

Factlog.prototype.onError = function (error) {
  console.warn("Factlog connection error; retrying in 2 seconds:", error)
  setTimeout(this.start.bind(this), 2000)
}

var foo = new Factlog(
  "http://localhost:8000/foo",
  20, { cool: 20 },
  function (fact) {
    if (fact == "cool")
      console.log(++this.cool)
  }
)

foo.start()

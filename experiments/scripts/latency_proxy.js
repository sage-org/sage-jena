#!/usr/bin/env node
/* file : load_proxy.js
MIT License

Copyright (c) 2017 Thomas Minier

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
'use strict'

const http = require('http')
const HttpProxy = require('http-proxy')

const proxyConfig = {
  target: 'http://172.16.8.50:8890'
}
const timeout = 2 * 60 * 60 * 1000
const proxyPort = '8000'
const delay = 50

const proxy = HttpProxy.createProxyServer({proxyTimeout: timeout})
const proxyServer = http.createServer((req, res) => {
  res.setTimeout(timeout)
  setTimeout(() => {
    proxy.web(req, res, proxyConfig)
  }, delay)
})

process.stdout.write(`Latency (delay: ${delay}) proxy up and running at http://localhost:${proxyPort}\n`)
proxyServer.listen(proxyPort)

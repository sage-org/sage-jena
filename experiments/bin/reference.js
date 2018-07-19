#!/usr/bin/env node
/* file : reference.js
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

const fs = require('fs')
const path = require('path')
const program = require('commander')
const ldf = require('ldf-client')
const Request = require('./Request.js')
ldf.Logger.setLevel('WARNING')

class Spy {
  constructor() {
    this.nbCalls = 0
    this.responseTimes = []
  }

  get avgResponseTime () {
    return this.responseTimes.reduce((x, y) => x + y, 0) / this.responseTimes.length
  }
}

// Command line interface to execute queries
program
  .description('Execute a SPARQL query using the reference TPF client')
  .usage('<server> [options]')
  .option('-q, --query <query>', 'evaluates the given SPARQL query')
  .option('-f, --file <file>', 'evaluates the SPARQL query in the given file')
  .option('-t, --timeout <timeout>', 'set SPARQL query timeout in milliseconds (default: 30mn)', 30 * 60 * 1000)
  .option('-m, --measure <output>', 'measure the query execution time (in seconds) & append it to a file', './execution_times_ref.csv')
  .option('-s, --silent', 'do not perform any measurement (silent mode)', false)
  .parse(process.argv)

// get servers
if (program.args.length !== 1) {
  process.stderr.write('Error: you must specify exactly one TPF server to use.\nSee ./bin/reference.js --help for more details.\n')
  process.exit(1)
}

const server = program.args[0]
const configFile = path.join(__dirname, '../node_modules/ldf-client/config-default.json')
const config = JSON.parse(fs.readFileSync(configFile, { encoding: 'utf8' }))

// fetch SPARQL query to execute
let query = null
let timeout = null
if (program.query) {
  query = program.query
} else if (program.file && fs.existsSync(program.file)) {
  query = fs.readFileSync(program.file, 'utf-8')
} else {
  process.stderr.write('Error: you must specify a SPARQL query to execute.\nSee ./bin/reference.js --help for more details.\n')
  process.exit(1)
}
config.fragmentsClient = new ldf.FragmentsClient(server, config)

// Spy used to measure the nb of HTTP calls
const spy = new Spy()
config.fragmentsClient._httpClient._request = Request(spy)

const iterator = ldf.SparqlIterator(query, config)
iterator.on('error', error => {
  process.stderr.write('ERROR: An error occurred during query execution.\n')
  process.stderr.write(error.stack)
})

iterator.on('end', () => {
  clearTimeout(timeout)
  if (!program.silent) {
    const endTime = Date.now()
    const time = endTime - startTime
    fs.appendFileSync(program.measure, `${time / 1000},${spy.nbCalls},${spy.avgResponseTime}`)
  }
})
const startTime = Date.now()
iterator.on('data', data => process.stdout.write(`${JSON.stringify(data, Object.keys(data).sort())}\n`))

// set query timeout
timeout = setTimeout(() => {
  iterator.close()
  process.stderr.write(`TIMEOUT EXCEEDED (${program.timeout}ms) - shutting down query processing...\n`)
}, program.timeout)

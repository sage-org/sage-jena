#!/usr/bin/env node
/* file : virtuoso.js
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
const request = require('request').forever({timeout: 1000, minSockets: 10})
const program = require('commander')

// Command line interface to execute queries
program
  .description('Execute a SPARQL query using a Virtuoso SPARQL endpoint')
  .usage('<server> [options]')
  .option('-q, --query <query>', 'evaluates the given SPARQL query')
  .option('-f, --file <file>', 'evaluates the SPARQL query in the given file')
  .option('-m, --measure <output>', 'measure the query execution time (in seconds) & append it to a file', './execution_times_virtuo.csv')
  .option('-s, --silent', 'do not perform any measurement (silent mode)', false)
  .parse(process.argv)

// get servers
if (program.args.length !== 1) {
  process.stderr.write('Error: you must specify exactly one SPARQL endpoint to use.\nSee ./bin/virtuoso.js --help for more details.\n')
  process.exit(1)
}

const server = program.args[0]

// fetch SPARQL query to execute
let query = null
if (program.query) {
  query = program.query
} else if (program.file && fs.existsSync(program.file)) {
  query = fs.readFileSync(program.file, 'utf-8')
} else {
  process.stderr.write('Error: you must specify a SPARQL query to execute.\nSee ./bin/virtuoso.js --help for more details.\n')
  process.exit(1)
}

const config = {
  method: 'GET',
  url: server,
  qs: {
    'default-graph-uri': 'http://localhost:8000/watdiv10m',
    query: query,
    format: 'application/sparql-results+json',
    timeout: 5 * 60 * 1000
  }
}

const startTime = Date.now()
request(config, (error, res, body) => {
  const endTime = Date.now()
  const time = endTime - startTime
  if (error) {
    process.stderr.write('ERROR: An error occurred during query execution.\n')
    process.stderr.write(error.stack)
  } else {
    const bindings = JSON.parse(body).results.bindings
    bindings.forEach(b => {
      process.stdout.write(`${JSON.stringify(b)}\n`)
    })
    if (!program.silent) {
      fs.appendFileSync(program.measure, `${time / 1000},1`)
    }
  }
})

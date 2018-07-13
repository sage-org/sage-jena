#!/usr/bin/env node
/* file : virtuoso-paginate.js
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
const { Parser, Generator } = require('sparqljs')
const request = require('request').forever({timeout: 1000, minSockets: 10})
const program = require('commander')
const { merge, uniq } = require('lodash')

let nbCalls = 0

function formatQuery (parsedQuery, limit = 2000) {
  const plan = merge({}, parsedQuery)
  let variables = []
  plan.where[0].triples.forEach(tp => {
    if (tp.subject.startsWith('?')) variables.push(tp.subject)
    if (tp.predicate.startsWith('?')) variables.push(tp.predicate)
    if (tp.object.startsWith('?')) variables.push(tp.object)
  })
  variables = uniq(variables)
  plan.order = variables.map(v => {
    return { expression: v }
  })
  plan.limit = limit
  plan.offset = 0
  return plan
}

function updateRetry (plan, server, allResults, generator) {
  plan.offset += plan.limit
  const query = generator.stringify(plan)
  return evalQuery(query, server)
    .then(results => {
      results.forEach(b => allResults.push(b))
      if (results.length < 2000) return Promise.resolve('done')
      return updateRetry(plan, server, allResults, generator)
    })
}

function evalQuery (query, url) {
  return new Promise((resolve, reject) => {
    const config = {
      method: 'GET',
      url,
      qs: {
        'default-graph-uri': 'http://localhost:8890/watdiv10m',
        query,
        format: 'application/sparql-results+json',
        timeout: 5 * 60 * 1000
      }
    }
    nbCalls++
    request(config, (error, res, body) => {
      if (error) {
        reject(error)
      } else {
        if (body.includes('Virtuoso')) {
          reject(new Error(body))
        } else {
          const bindings = JSON.parse(body).results.bindings.map(b => `${JSON.stringify(b)}\n`)
          resolve(bindings)
        }
      }
    })
  })
}

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
  process.stderr.write('Error: you must specify exactly one TPF server to use.\nSee ./bin/reference.js --help for more details.\n')
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
  process.stderr.write('Error: you must specify a SPARQL query to execute.\nSee ./bin/reference.js --help for more details.\n')
  process.exit(1)
}

const parser = new Parser()
const generator = new Generator()
let parsedQuery = parser.parse(query)
parsedQuery = formatQuery(parsedQuery)

let bindings = []
const startTime = Date.now()
evalQuery(generator.stringify(parsedQuery), server)
  .then(results => {
    results.forEach(b => bindings.push(b))
    if (results.length < 2000) return Promise.resolve('done')
    return updateRetry(parsedQuery, server, bindings, generator)
  })
  .then(x => {
    bindings.forEach(b => {
      process.stdout.write(`${JSON.stringify(b)}\n`)
    })
    const endTime = Date.now()
    const time = endTime - startTime
    if (!program.silent) {
      fs.appendFileSync(program.measure, `${time / 1000},${nbCalls}`)
    }
  })
  .catch(error => {
    process.stderr.write(error.stack)
    bindings.forEach(b => {
      process.stdout.write(`${JSON.stringify(b)}\n`)
    })
    const endTime = Date.now()
    const time = endTime - startTime
    if (!program.silent) {
      fs.appendFileSync(program.measure, `${time / 1000},${nbCalls}`)
    }
  })

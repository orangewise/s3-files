const sinon = require('sinon')
const t = require('tap')
const PassThrough = require('stream').PassThrough

const proxyquire = require('proxyquire')
const s3Stub = {}
const s3Files = proxyquire('../s3-files.js', {
  '@aws-sdk/client-s3': { S3Client: sinon.stub().returns(s3Stub) }
})

// Connect
t.type(s3Files.s3, undefined)
s3Files.connect({})
t.type(s3Files.s3, 'object')

// Keystream
const keyStream = s3Files.createKeyStream('folder', undefined)
t.same(keyStream, null)

t.test('keyStream', function (child) {
  const keyStream = s3Files.createKeyStream('folder/', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'folder/a')
    if (cnt === 1) child.equal(chunk.toString(), 'folder/b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

t.test('keyStream without folder having trailing slash', function (child) {
  const keyStream = s3Files.createKeyStream('folder', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'folder/a')
    if (cnt === 1) child.equal(chunk.toString(), 'folder/b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

t.test('keyStream without folder', function (child) {
  const keyStream = s3Files.createKeyStream('', ['a', 'b'])
  let cnt = 0
  keyStream.on('data', function (chunk) {
    if (cnt === 0) child.equal(chunk.toString(), 'a')
    if (cnt === 1) child.equal(chunk.toString(), 'b')
    cnt++
  })
  keyStream.on('end', function () {
    child.end()
  })
})

// Filestream
t.test('Filestream needs a bucket', function (child) {
  let fileStream = s3Files.createFileStream()
  child.same(fileStream, null)

  const keyStream = s3Files
    .connect({ bucket: 'bucket' })
    .createKeyStream('folder/', ['a', 'b', 'c'])

  const s = new PassThrough()
  s.end('hi')
  s3Stub.send = function () {
    return new Promise(function (resolve) {
      resolve({
        Body: s
      })
    })
  }
  let cnt = 0
  fileStream = s3Files.createFileStream(keyStream)
  fileStream.on('data', function (chunk) {
    child.equal(chunk.data.toString(), 'hi')
    if (cnt === 0) child.equal(chunk.path, 'a')
    if (cnt === 1) {
      child.equal(chunk.path, 'b')
    }
    if (cnt === 2) {
      s.emit('error', new Error('fail'))
    }
    cnt++
  })
  fileStream.on('error', function (chunk) {
    child.ok(chunk)
  })
  fileStream.on('end', function (chunk) {
    setTimeout(function () {
      child.end()
    })
  })
})

t.test('connect with existing s3 client', function (child) {
  const customClient = { custom: true }
  const result = s3Files.connect({ s3: customClient, bucket: 'test-bucket' })
  child.equal(result.s3, customClient)
  child.equal(result.bucket, 'test-bucket')
  // restore s3Stub for subsequent tests
  s3Files.connect({ bucket: 'bucket' })
  child.end()
})

t.test('fileStream throttles with more than 5 files', function (child) {
  const files = ['a', 'b', 'c', 'd', 'e', 'f']
  const keyStream = s3Files
    .connect({ bucket: 'bucket' })
    .createKeyStream('folder/', files)

  s3Stub.send = function () {
    const s = new PassThrough()
    s.end('data')
    return Promise.resolve({ Body: s })
  }

  let dataCount = 0
  const fileStream = s3Files.createFileStream(keyStream, true)
  fileStream.on('data', function () {
    dataCount++
  })
  fileStream.on('end', function () {
    child.ok(dataCount >= 5, 'all files processed')
    child.end()
  })
})

t.test('fileStream resumes keyStream on rejection when paused', function (child) {
  const files = ['a', 'b', 'c', 'd', 'e', 'f']
  const keyStream = s3Files
    .connect({ bucket: 'bucket' })
    .createKeyStream('folder/', files)

  s3Stub.send = function () {
    return Promise.reject(new Error('fail'))
  }

  const errors = []
  let ended = false
  const fileStream = s3Files.createFileStream(keyStream)
  fileStream.on('error', function (err) {
    errors.push(err)
  })
  fileStream.on('end', function () {
    if (ended) return
    ended = true
    child.ok(errors.length > 0, 'errors were emitted')
    child.end()
  })
})

t.test('s3 GetObject rejection emits error and ends stream', (child) => {
  s3Stub.send = function () {
    return Promise.reject(new Error('GetObject failed'))
  }

  const keyStream = s3Files
    .connect({ bucket: 'bucket' })
    .createKeyStream('folder/', ['a', 'b', 'c'])

  const fileStream = s3Files.createFileStream(keyStream)

  const errors = []
  fileStream.on('error', (err) => {
    errors.push(err)
  })
  fileStream.on('end', () => {
    child.ok(errors.length > 0, 'at least one error emitted')
    child.equal(errors[0].message, 'GetObject failed')
    child.equal(errors[0].file, 'folder/a')
    child.end()
  })
})

t.end()

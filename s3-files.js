const Stream = require('stream')
const { S3Client } = require('@aws-sdk/client-s3')
const streamify = require('stream-array')
const concat = require('concat-stream')
const path = require('path')

const s3Files = {}
module.exports = s3Files

s3Files.connect = function (opts) {
  const self = this

  if ('s3' in opts) {
    self.s3 = opts.s3
  } else {
    self.s3 = new S3Client({
      region: opts.region
    })
  }

  self.bucket = opts.bucket
  return self
}

s3Files.createKeyStream = function (folder, keys) {
  if (!keys) return null
  const paths = []
  keys.forEach(function (key) {
    if (folder) {
      paths.push(path.posix.join(folder, key))
    } else {
      paths.push(key)
    }
  })
  return streamify(paths)
}

s3Files.createFileStream = function (keyStream, preserveFolderPath) {
  const self = this
  if (!self.bucket) return null

  const rs = new Stream()
  rs.readable = true

  let fileCounter = 0
  keyStream.on('data', function (file) {
    fileCounter += 1
    if (fileCounter > 5) {
      keyStream.pause() // we add some 'throttling' there
    }

    // console.log('->file', file);
    const params = { Bucket: self.bucket, Key: file }
    const s3File = self.s3.getObject(params).createReadStream()

    s3File.pipe(
      concat(function buffersEmit (buffer) {
        // console.log('buffers concatenated, emit data for ', file);
        const path = preserveFolderPath ? file : file.replace(/^.*[\\/]/, '')
        rs.emit('data', { data: buffer, path })
      })
    )
    s3File.on('end', function () {
      fileCounter -= 1
      if (keyStream.isPaused()) {
        keyStream.resume()
      }
      if (fileCounter < 1) {
        // console.log('all files processed, emit end');
        rs.emit('end')
      }
    })

    s3File.on('error', function (err) {
      err.file = file
      rs.emit('error', err)
    })
  })
  return rs
}

# s3-files

[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Coverage Status][coveralls-badge]][coveralls-url]
[![JavaScript Style Guide](https://img.shields.io/badge/code%20style-standard-brightgreen.svg)](http://standardjs.com/)


Stream selected files from an Amazon s3 bucket/folder.


## Install

```
npm install s3-files
```


## AWS Configuration

Refer to the [AWS SDK][aws-sdk-url] for authenticating to AWS prior to using this plugin.


## Usage: Stream files from a bucket folder

```javascript

var s3Files = require('s3-files')

var region = 'bucket-region'
var bucket = 'name-of-s3-bucket'
var folder = 'name-of-bucket-folder/'
var file1 = 'Image A.png'
var file2 = 'Image B.png'
var file3 = 'Image C.png'
var file4 = 'Image D.png'

// Create a stream of keys.
var keyStream = s3Files
  .connect({
    region: region,
    bucket: bucket    
  })
  .createKeyStream(folder, [file1, file2, file3, file4])

// Stream the files.
s3Files.createFileStream(keyStream)
  .on('data', function (chunk) {
    console.log(chunk.path, chunk.data.length)
  })
```

## Usage: Stream files from the root of a bucket

```javascript

var s3Files = require('s3-files')

var region = 'bucket-region'
var bucket = 'name-of-s3-bucket'
var folder = ''
var file1 = 'Image A.png'
var file2 = 'Image B.png'
var file3 = 'Image C.png'
var file4 = 'Image D.png'

// Create a stream of keys.
var keyStream = s3Files
  .connect({
    region: region,
    bucket: bucket    
  })
  .createKeyStream(folder, [file1, file2, file3, file4])

// Stream the files.
s3Files.createFileStream(keyStream)
  .on('data', function (chunk) {
    console.log(chunk.path, chunk.data.length)
  })
```



## Testing

Tests are written in Node Tap, run them like this:

```
npm t
```

If you would like a more fancy report:

```
npm test -- --cov --coverage-report=lcov
```


[aws-sdk-url]: https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/configuring-the-jssdk.html
[npm-badge]: https://badge.fury.io/js/s3-files.svg
[npm-url]: https://badge.fury.io/js/s3-files
[travis-badge]: https://travis-ci.org/orangewise/s3-files.svg?branch=master
[travis-url]: https://travis-ci.org/orangewise/s3-files
[coveralls-badge]: https://coveralls.io/repos/github/orangewise/s3-files/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/orangewise/s3-files?branch=master

/** @license MIT License (c) copyright 2014 original authors */
/** @author Karolis Narkevicius */

var path = require('path');
var knox = require('knox');
var when = require('when');
var thunkify = require('thunkify');
var saveTo = thunkify(require('save-to'));

module.exports = function (config) {
  return new S3(config);
};

/**
 * Amazon S3 Storage Adapter for CNPM
 *
 * @param {Object} knox config
 * @api public
 */

function S3(config) {
  this.config = config;
  this.client = knox.createClient(this.config);
  this.getFile = thunkify(this.client.getFile);
  this.deleteFile = thunkify(this.client.deleteFile);
  this.listFunc = thunkify(this.client.list);
}

/**
 * Upload a package from filepath to S3.
 *
 * @param {String} filepath the path of the file to upload
 * @param {Object} options with key and size
 * @return {Object} an object with the key
 * @api public
 */

S3.prototype.upload = function* (filepath, options) {
  var s3Config = this.config;
  var client = this.client;
  var dest = this.getPath(options.key);

  var uploadOptions = {};

  if (s3Config.storageClass) {
    uploadOptions['x-amz-storage-class'] = s3Config.storageClass;
  }

  yield when.promise(function (resolve, reject) {
    client.putFile(filepath, dest, uploadOptions, function (err, res) {
      if (err) return reject(err);
      if (res.statusCode !== 200) { return reject(new Error('putFile failed with ' + res.statusCode)); }
      resolve();
    }).on('error', function (err) {
      reject(new Error('Network error' + err.message));
    });
  });

  return { key: options.key };
};

/**
 * Upload a package from filepath to S3.
 *
 * @param {String} contents of the file to upload
 * @param {Object} options with key and size
 * @return {Object} an object with the key
 * @api public
 */

S3.prototype.uploadBuffer = function* (content, options) {
  var client = this.client;
  var filepath = this.getPath(options.key);

  var headers = {
    'Content-Type': 'application/x-gzip'
  };
  yield when.promise(function (resolve, reject) {
    client.putBuffer(content, filepath, headers, function(err, res) {
      if (err) return reject(err);
      if (res.statusCode !== 200) return reject(new Error('putBuffer failed with ' + res.statusCode));
      resolve();
    }).on('error', function (err) {
      reject(new Error('Network error' + err.message));
    });
  });

  return { key: options.key };
};

/**
 * Download a package from S3.
 *
 * @param {String} package key
 * @param {String} download path
 * @param {options} an object with timeout
 * @api public
 */

S3.prototype.download = function* (key, savePath) {
  var filepath = this.getPath(key);
  var res = yield this.getFile.call(this.client, filepath);
  yield saveTo(res, savePath);
};

/**
 * Remove a package from S3
 *
 * @param {String} package key
 * @api public
 */

S3.prototype.remove = function* (key) {
  var filepath = this.getPath(key);
  yield this.deleteFile.call(this.client, filepath);
};

/**
*
* @param {options} prefix
* @api public
*/

S3.prototype.list = function*(params){
  if(!params) params = {};
  return yield this.listFunc.call(this.client, params);
}

/**
*
* @param {options} prefix
* @api public
*/

S3.prototype.listAll = function*(params){
  if(!params) params = {};
  var objList = {};
  var truncated = true;
  params.marker = '';
  var j = 0;
  while(truncated){
    var resp = yield this.listFunc.call(this.client, params);
    for(var i = 0; i < resp.Contents.length; i ++){
      objList[resp.Contents[i].Key] = resp.Contents[i].Key;
      j++;

      if(i === resp.Contents.length - 1) params.marker = resp.Contents[i].Key;
    }
    truncated = (resp.IsTruncated == true);

  }
  return objList;
}

/**
 * escape '/' and '\'
 * prepend the config.folder
 */

S3.prototype.getPath = function (key) {
  key = key.replace(/\//g, '-').replace(/\\/g, '_');
  key = path.join(this.config.folder, key);
  return key;
};


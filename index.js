
const DynamoDB = function () {
  this.client = null
  this.AWS = require('aws-sdk')
}

DynamoDB.prototype.setConfiguration = function (configFile, env) {
  if (configFile.environments[env].provider !== 'aws') {
    throw new Error('The configuration provider must be of type aws')
  }
  this.configuration = configFile.environments[env].configuration
  this.AWS.config.update(this.configuration)
  this.instantiateClient()
}

DynamoDB.prototype.instantiateClient = function () {
  this.client = new this.AWS.DynamoDB.DocumentClient({apiVersion: '2012-10-08'})
}

DynamoDB.prototype.query = function (tableName = null, key, callback, options = {}) {
  const basic = {
    Key: key,
    TableName: tableName,
  }
  
  const params = Object.assign(basic, options); 

  this.client.query(params, callback)
}

DynamoDB.prototype.getItem = function (tableName, key, callback, options = {}) {
  const basic = {
    TableName: tableName,
    Key: key
  }
  const params = Object.assign(basic, options);

  this.client.get(params, callback)
}

DynamoDB.prototype.putItem = function (tableName, item, callback, options = {}) {
  const basic = {
    TableName: tableName,
    Item: item
  }
  const params = Object.assign(basic, options);
  this.client.put(params, callback)
}

DynamoDB.prototype.deleteItem = function (tableName, key, callback) {
  const params = {
    TableName: tableName,
    Key: key
  }

  this.client.delete(params, callback)
}

module.exports = DynamoDB

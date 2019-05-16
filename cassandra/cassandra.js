var connections = {};

function createConnection(cassandra, connectionOptions, callback) {
  var key = JSON.stringify(connectionOptions);
  if (connections[key]) {
    connections[key](callback);
  } else {
    var waiting = [callback];
    connections[key] = (connectCallback) => {
      waiting.push(connectCallback);
    };

    function reconnect() {
      var connection = new cassandra.Client(connectionOptions);
      connection.connect((err) => {
        if (err) {
          console.log(connectionOptions);
          console.error(err);
          waiting.forEach(w => w(err));
          setTimeout(reconnect, 30000);
        } else {
          connections[key] = c => c(null, connection);
          waiting.forEach(w => w(null, connection));
          waiting = [];
        }
      });
    }

    reconnect();
  }
}

module.exports = function (RED) {
  "use strict";
  var cassandra = require('cassandra-driver');

  function CassandraNode(n) {
    RED.nodes.createNode(this, n);
    this.hosts = n.hosts;
    this.port = n.port;

    this.connected = false;
    this.connecting = false;

    this.keyspace = n.keyspace;
    this.localDataCenter = n.localDataCenter;
    var node = this;

    function doConnect() {
      node.connecting = true;

      var authProvider = null;
      if (node.credentials.user) {
        authProvider = new cassandra.auth.PlainTextAuthProvider(
          node.credentials.user,
          node.credentials.password
        );
      }

      // TODO: Support other port, default is 9042
      var connectionOptions = {
        contactPoints: node.hosts.replace(/ /g, "").split(","),
        localDataCenter: node.localDataCenter,
        keyspace: node.keyspace,
        authProvider: authProvider,
      };
      if (node.port) {
        connectionOptions.protocolOptions = {port: node.port};
      }
      if (node.localDataCenter) {
        const loadBalancingPolicy = new cassandra.policies.loadBalancing.DCAwareRoundRobinPolicy(node.localDataCenter);
        connectionOptions.policies = {
          loadBalancing: loadBalancingPolicy
        };
      }

      createConnection(cassandra, connectionOptions, (err, connection) => {
        if (err) {
          node.error(err);
        } else {
          node.connecting = false;
          node.connection = connection;
          node.connected = true;
        }
      });
    }

    this.connect = function () {
      if (!this.connected && !this.connecting) {
        doConnect();
      }
    }

    this.on('close', function (done) {
      if (this.tick) { clearTimeout(this.tick); }
      if (this.connection) {
        node.connection.shutdown(function (err) {
          if (err) { node.error(err); }
          done();
        });
      } else {
        done();
      }
    });
  }
  RED.nodes.registerType("CassandraDatabase",CassandraNode, {
    credentials: {
      user: {type: "text"},
      password: {type: "password"}
    }
  });


  function CassandraNodeIn(n) {
    RED.nodes.createNode(this,n);
    this.mydb = n.mydb;
    this.mydbConfig = RED.nodes.getNode(this.mydb);

    if (this.mydbConfig) {
      this.mydbConfig.connect();
      var node = this;
      this.on("input", function(msg) {
        var batchMode = Array.isArray(msg.topic);
        if (!batchMode && typeof msg.topic !== 'string') {
          node.error("msg.topic : the query is not defined as a string or as an array of queries");
          return;
        }
        var resCallback = function(err, result) {
          if (err) {
            node.error(err,msg);
          } else {
            msg.payload = result.rows;
            node.send(msg);
          }
        };

        if (batchMode) {
          node.log("Batching " + msg.topic.length + " CQL queries");
          node.mydbConfig.connection.batch(msg.topic, {prepare: true}, resCallback);
        } else {
          node.log("Executing CQL query: ", msg.topic);
          var params = msg.payload || [];
          node.mydbConfig.connection.execute(msg.topic, params, {prepare: true}, resCallback);
        }
      });
    }
    else {
      this.error("Cassandra database not configured");
    }
  }
  RED.nodes.registerType("cassandra", CassandraNodeIn);
}

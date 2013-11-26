var Job = require('./job.js'),
    humanInterval = require('human-interval'),
    utils = require('util'),
    Emitter = require('events').EventEmitter,
    mongo = require('mongodb');

var Agenda = module.exports = function(config) {
  if(!(this instanceof Agenda)) return new Agenda(config);
  if(!config) config = {};
  this._processEvery = humanInterval(config.processEvery) || humanInterval('5 seconds');
  this._defaultConcurrency = config.defaultConcurrency || 5;
  this._maxConcurrency = config.maxConcurrency || 20;
  this._definitions = {};
  this._runningJobs = 0;
  this._dbPendingFn = [ ];
  if(config.db)
    this.database(config.db.address, config.db.collection, config.db.database);
};

utils.inherits(Agenda, Emitter);

// Configuration Methods

Agenda.prototype.database = function(url, collection, database) {
  this._db   = null;
  collection = collection || 'agendaJobs';

  var self = this;
  mongo.connect(url, { db: { w: true } }, function(err, db) {
    if(null != err) throw err

    if(null != database) {
      db = db.db(database);
    }
    self._db = db.collection(collection);

    // Run pending jobs.
    var fn;
    while(null != (fn = self._dbPendingFn.shift())) {
      fn();
    }
  });

  return this;
};

Agenda.prototype.processEvery = function(time) {
  this._processEvery = humanInterval(time);
  return this;
};

Agenda.prototype.maxConcurrency = function(num) {
  this._maxConcurrency = num;
  return this;
};

Agenda.prototype.defaultConcurrency = function(num) {
  this._defaultConcurrency = num;
  return this;
};

// Job Methods
Agenda.prototype.create = function(name, data) {
  var priority = this._definitions[name] ? this._definitions[name].priority : 0;
  var job = new Job({name: name, data: data, type: 'normal', priority: priority, agenda: this});
  return job;
};

Agenda.prototype.jobs = function() {
  var args = Array.prototype.slice.call(arguments);
  var self = this;

  // If there is no database connection yet, add to pending.
  if(null === this._db) {
    this._dbPendingFn.push(function() {
      return self.jobs.apply(self, args);
    });
    return;
  }

  if(typeof args[args.length -1] == 'function') {
    var fn = args.pop();
    var wrapJobs = function(err, jobs) {
      if(err) fn(err, null);
      else {
        jobs = jobs.map(function(j) {
          j.agenda = self;
          return new Job(j);
        });
        fn(err, jobs);
      }
    };
  }

  return this._db.find.apply(this._db, args).toArray(wrapJobs);
};

Agenda.prototype.define = function(name, options, processor) {
  if(!processor) {
    processor = options;
    options = {};
  }
  this._definitions[name] = {
   fn: processor,
   concurrency: options.concurrency || this._defaultConcurrency,
   priority: options.priority || 0,
   running: 0
  };
};

Agenda.prototype.every = function(interval, name, data) {
  var job;
  job = this.create(name, data);
  job.attrs.type = 'single';
  job.repeatEvery(interval);
  job.save();
  return job;
};

Agenda.prototype.schedule = function(when, name, data) {
  var job = this.create(name, data);
  job.schedule(when);
  job.save();
  return job;
};

Agenda.prototype.saveJob = function(job, cb) {
  // If there is no database connection yet, add to pending.
  if(null === this._db) {
    var self = this;
    this._dbPendingFn.push(function() {
      return self.saveJob(job, cb);
    });
    return;
  }


  var fn = cb;
  var props = getJobProperties(job);
  if(props.type == 'single')
    this._db.findAndModify({name: props.name, type: 'single'}, {}, {$set: props}, {upsert: true, new: true}, processDbResult);
  else {
    if(job.attrs._id) {
      this._db.findAndModify({_id: job.attrs._id}, {}, {$set: props}, {new: true}, processDbResult);
    }
    else {
      this._db.insert(props, processDbResult);
    }
  }

  function processDbResult(err, res) {
    if(err) throw(err);
    else if(res) {
      if (Array.isArray(res)) {
        job.attrs._id = res[0]._id;
      } else if(typeof res == 'object') {
        job.attrs._id = res._id;
      }
    }

    if(fn)
      fn(err, job);
  }
};

// Job Flow Methods

Agenda.prototype.start = function() {
  var self = this;
  if(!this._processInterval) {
    this._processInterval = setInterval(function() {
      processJobs.call(self);
    }, this._processEvery);
  }
};

Agenda.prototype.stop = function() {
  clearInterval(this._processInterval);
  this._processInterval = undefined;
};

function processJobs() {
  var definitions = this._definitions,
      self = this;
  var now = new Date();
  this.jobs({nextRunAt: {$lte: now}}, {sort: {'priority': -1}}, function(err, jobs) {
    if(err) throw(err);
    else fillJobQueue();

    function fillJobQueue() {
      if(!jobs.length) return;
      while(self._runningJobs < self._maxConcurrency) {
        var job = getNextProcessableJob();
        if(job) {
          job.run(fillJobQueue);
        } else
          break;
      }
    }

    function getNextProcessableJob() {
      var definition, job, index;
      for(index = 0; index < jobs.length; ++index) {
        definition = definitions[jobs[index].attrs.name];
        if(!definition) throw new Error("Attempted to run job undefined job: " + jobs[index].attrs.name);
        if(definition.concurrency > definition.running) {
          job = jobs[index];
          break;
        }
      }
      if(job) return jobs.splice(index, 1)[0];
      return undefined;
    }
  });
}

function getJobProperties(job) {
  var props = {};
  for(var key in job.attrs) {
    props[key] = job.attrs[key];
  }
  delete props._id;
  return props;
}

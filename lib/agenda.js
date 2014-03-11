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
  else if(config.mongo)
    this._db =  config.mongo;
};

utils.inherits(Agenda, Emitter);

// Configuration Methods

Agenda.prototype.mongo = function(db) {
  this._db = db;
};

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

  var wrap;
  if(typeof args[args.length - 1] == 'function') {
    wrap = findJobsResultWrapper(this, args.pop());
  }
  return this._db.find.apply(this._db, args).toArray(wrap);
};

Agenda.prototype.purge = function(cb) {
  // If there is no database connection yet, add to pending.
  var self = this;
  if(null === this._db) {
    this._dbPendingFn.push(function() {
      return self.purge.apply(self, args);
    });
    return;
  }

  var definedNames = Object.keys(this._definitions);
  this._db.remove({name: {$not: {$in: definedNames}}}, cb);
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
    lockLifetime: options.lockLifetime || 10 * 60 * 1000,//10 minute default lockLifetime
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

Agenda.prototype.now = function(name, data) {
  var job = this.create(name, data);
  job.schedule(new Date());
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

  var props = job.toJSON(),
      newOrUnloaded = typeof props._id == 'undefined';

  delete props._id;


  if(props.type == 'single') {
    var preservedProps = {};

    if(newOrUnloaded) {
      var keep = [ 'lastRunAt', 'lastFinishedAt', 'nextRunAt', 'failReason', 'failedAt', 'lockedAt'];
      keep.forEach(function(prop) {
        preservedProps[prop] = props[prop];
        delete props[prop];
      });
    }
    this._db.findAndModify({name: props.name, type: 'single'}, {}, {$set: props, $setOnInsert: preservedProps}, {upsert: true, new: true}, processDbResult);
  } else {
    if(job.attrs._id) {
      try {
        job.attrs._id = mongo.ObjectID(job.attrs._id);
      }
      catch(e) { }

      this._db.findAndModify({_id: job.attrs._id}, {}, {$set: props}, {new: true}, processDbResult);
    }
    else {
      this._db.insert(props, processDbResult);
    }
  }

  function processDbResult(err, res) {
    if(err) throw(err);
    else if(res) {
      if(Array.isArray(res)) {
        job.attrs._id = res[0]._id;
      } else if(typeof res == 'object') {
        job.attrs._id = res._id;
      }
    }

    if(fn) {
      fn(err, job);
    }
  }
};

// Job Flow Methods

Agenda.prototype.start = function() {
  var self = this;
  if(!this._processInterval) {
    this._processInterval = setInterval(processJobs.bind(this), this._processEvery);
    process.nextTick(processJobs.bind(this));
  }
};

Agenda.prototype.stop = function() {
  clearInterval(this._processInterval);
  this._processInterval = undefined;
};

/**
 * Find and lock jobs
 * @param {String} jobName
 * @param {Function} cb
 * @protected
 */
Agenda.prototype._findAndLockNextJob = function(jobName, definition, cb) {

  // If there is no database connection yet, add to pending.
  var self = this;
  if(null === this._db) {
    this._dbPendingFn.push(function() {
      return self._findAndLockNextJob.apply(self, args);
    });
    return;
  }

  var now = new Date(),
      lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);

  this._db.findAndModify(
    {
      nextRunAt: {$lte: now},
      $or: [
        {lockedAt: null},
        {lockedAt: {$exists: false}},
        {lockedAt: {$lte: lockDeadline}}
      ],
      name: jobName
    },
    {'priority': -1},
    {$set: {lockedAt: now}},
    {'new': true},
    findJobsResultWrapper(this, cb)
  );
};

/**
 *
 * @param agenda
 * @param cb
 * @return {Function}
 * @private
 */
function findJobsResultWrapper(agenda, cb) {
  return function (err, jobs) {
    if(jobs) {
      //query result can be array or one record
      if(jobs instanceof Array) {
        jobs = jobs.map(createJob.bind(null, agenda));
      } else {
        jobs = createJob(agenda, jobs);
      }
    }

    cb(err, jobs);
  };
}

/**
 * Create Job object from data
 * @param {Object} agenda
 * @param {Object} jobData
 * @return {Job}
 * @private
 */
function createJob(agenda, jobData) {
  jobData.agenda = agenda;
  return new Job(jobData);
}

function processJobs() {
  this.emit('process', this);

  var definitions = this._definitions,
    jobName,
    jobQueue = [],
    self = this;

  for (jobName in definitions) {
    jobQueueFilling(jobName);
  }

  function jobQueueFilling(name) {
    self._findAndLockNextJob(name, definitions[name], function (err, job) {
      if(err) {
        throw err;
      }

      if(job) {
        jobQueue.push(job);
        jobQueueFilling(name);
        jobProcessing();
      }
    });
  }

  function jobProcessing() {
    if(!jobQueue.length){
      return;
    }

    var job = jobQueue.pop(),
      name = job.attrs.name,
      jobDefinition = definitions[name];

    if(jobDefinition.concurrency > jobDefinition.running &&
      self._runningJobs < self._maxConcurrency) {

      self._runningJobs++;
      jobDefinition.running++;

      job.run(processJobResult);
      jobProcessing();
    } else {
      jobQueue.unshift(job);
    }
  }

  function processJobResult(err, job) {
    var name = job.attrs.name;

    self._runningJobs--;
    definitions[name].running--;

    jobProcessing();
  }
}

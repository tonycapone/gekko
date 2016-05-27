// 
// The fetcher is responsible for fetching new 
// trades at the exchange. It will emit `new trades`
// events as soon as it fetches new data.
// 
// How often this is depends on:
// 
//  - The capability of the exchange (to provide
//  historical data).
//  - The amount of data we get per fetch.
//  - The interval at which we need new data.

var _ = require('lodash');
var moment = require('moment');
var async = require('async');
var utc = moment.utc;
var log = require('./log.js');

var util = require('./util');
var config = util.getConfig();

var exchangeChecker = require('./exchangeChecker');
var CandleManager = require('./candleManager');

var provider = config.watch.exchange.toLowerCase();
var DataProvider = require('../exchanges/' + provider);

var Fetcher = function() {
  _.bindAll(this);

  // Create a public dataProvider object which can retrieve live 
  // trade information from an exchange.

  this.watcher = new DataProvider(config.watch);
  this.lastFetch = false;

  this.exchange = exchangeChecker.settings(config.watch);
  this.historicalStart = this.exchange.providesHistory;

  this.pair = [
    config.watch.asset,
    config.watch.currency
  ].join('/');

  // console.log(config);
  log.info('Starting to watch the market:',
    this.exchange.name,
    this.pair
  );

  this.on('new trades', function(a) {
    log.debug(
      'Fetched',
      _.size(a.all),
      'new trades, from',
      a.start.format('YYYY-MM-DD HH:mm:ss UTC'),
      'to',
      a.end.format('YYYY-MM-DD HH:mm:ss UTC')
    );
  });  
}

var Util = require('util');
var EventEmitter = require('events').EventEmitter;
Util.inherits(Fetcher, EventEmitter);

Fetcher.prototype.start = function() {
  

  // if this exchange support historical trades
  // start fetching the data needed for history.
  var missing = GLOBAL.emitters.market.model.history.toFetch;
  if (this.historicalStart && missing > 0) {
    var since = (config.tradingAdvisor.candleSize * (config.tradingAdvisor.historySize + 1));
    this.fetchHistorical(moment().subtract(since, 'minutes'));
  } else {
    this.fetch(false);
  }
}

// Set the first & last trade date and set the
// timespan between them.
Fetcher.prototype.setFetchMeta = function(trades) {
  this.firstTrade = _.first(trades); 
  this.first = moment.unix(this.firstTrade.date).utc();
  this.lastTrade = _.last(trades);
  this.last = moment.unix(this.lastTrade.date).utc();

  this.fetchTimespan = util.calculateTimespan(this.first, this.last);
}

// *This method is only used if this exchange does not support
// historical data.*
// 
// we need to keep polling exchange because we cannot
// access older data. We need to calculate how often we
// we should poll.
// 
// Returns amount of ms to wait for until next fetch.
Fetcher.prototype.calculateNextFetch = function(trades) {

  // for now just refetch every minute
  return this.fetchAfter = util.minToMs(0.8);


  // not used at this moment

  // if the timespan per fetch is fixed at this exchange,
  // just return that number.
  if(this.exchange.fetchTimespan) {
    // todo: if the interval doesn't go in
    // sync with exchange fetchTimes we
    // need to calculate overlapping times.
    // 
    // eg: if we can fetch every 60 min but user
    // interval is at 80, we would also need to
    // fetch again at 80 min.
    var min = _.min([
      this.exchange.fetchTimespan,
      config.tradingAdvisor.candleSize
    ]);
    this.fetchAfter = util.minToMs(min);
    // debugging bitstamp
    this.fetchAfter = util.minToMs(1);
    return;  
  }
    
  var minimalInterval = util.minToMs(config.tradingAdvisor.candleSize);

  // if we got the last 100 seconds of trades last
  // time make sure we fetch at least in 55 seconds
  // again.
  var safeTreshold = 0.2;
  var defaultFetchTime = util.minToMs(1);

  if(this.fetchTimespan * safeTreshold > minimalInterval)
    // If the oldest trade in a fetch call > candle size
    // we can just use candle size.
    var fetchAfter = minimalInterval;
  else if(this.fetchTimespan * safeTreshold < defaultFetchTime)
    // If the oldest trade in a fetch call < default time
    // we fetch at default time.
    var fetchAfter = defaultFetchTime;
  else
    // use a safe fetch time to determine
    var fetchAfter = this.fetchTimespan * safeTreshold;

  this.fetchAfter = fetchAfter;
}

Fetcher.prototype.scheduleNextFetch = function() {
  setTimeout(this.fetch, this.fetchAfter);
}

Fetcher.prototype.fetch = function(since) {
  log.debug('Requested', this.pair ,'trade data from', this.exchange.name, '...');
  this.watcher.getTrades(since, this.processTrades, false);
  // this.spoofTrades();
}

Fetcher.prototype.fetchHistorical = function(since) {
  log.debug('Historical', this.pair ,'trade data from', this.exchange.name, '...');
  this.watcher.getTrades(since, this.processTradesHistory, false);
  // this.spoofTrades();
}

Fetcher.prototype.spoofTrades = function() {
  var fs = require('fs');
  trades = JSON.parse( fs.readFileSync('./a3.json', 'utf8') );
  this.processTrades(false, trades);

  setTimeout(this.spoofTrades2, 5000);
}

Fetcher.prototype.spoofTrades2 = function() {
  var fs = require('fs');
  trades = JSON.parse( fs.readFileSync('./a4.json', 'utf8') );
  this.processTrades(false, trades);
}

Fetcher.prototype.processTrades = function(err, trades) {
  if(err)
    throw err;

  // Make sure we have trades to process
  if(_.isEmpty(trades)) {
    log.debug('Trade fetch came back empty. Rescheduling...');
    this.calculateNextFetch();
    this.scheduleNextFetch();
    return;
  }

  this.setFetchMeta(trades);
  this.calculateNextFetch();

  // schedule next fetch
  this.scheduleNextFetch();

  this.emit('new trades', {
    timespan: this.fetchTimespan,
    start: this.first,
    first: this.firstTrade,
    end: this.last,
    last: this.lastTrade,
    all: trades,
    nextIn: this.fetchAfter
  });
}


Fetcher.prototype.processTradesHistory = function(err, trades) {
  if(err)
    throw err;

  this.setFetchMeta(trades);

  log.debug(
    'Fetched',
    _.size(trades),
    'historical trades, from',
    this.first.format('YYYY-MM-DD HH:mm:ss UTC'),
    'to',
    this.last.format('YYYY-MM-DD HH:mm:ss UTC')
  );

  // split trades into unique days
  var tradeDays = [[]],
      start = this.first.startOf('day').format('X'),
      end = this.first.endOf('day').format('X');
  _.each(trades, function(trade) {
    // trade has a new day
    if (!(start <= trade.date && trade.date <= end)) {
      // get first trade into this block
      // so system reconize two-day batch
      _.last(tradeDays).push(trade);

      // start new trade day
      tradeDays.push([]);
      var newdate = moment.unix(trade.date).utc();
      start = newdate.startOf('day').format('X');
      end = newdate.endOf('day').format('X');
    }

    _.last(tradeDays).push(trade);
  });

  // process each trade day
  async.eachSeries(tradeDays,
    _.bind(function(trades, next) {
      this.setFetchMeta(trades);

      var fetch = {
        timespan: this.fetchTimespan,
        start: this.first,
        first: this.firstTrade,
        end: this.last,
        last: this.lastTrade,
        all: trades,
        nextIn: this.fetchAfter
      };

      console.log();
      log.debug('Processing historical',
        this.first.format('YYYY-MM-DD'),
        'trades', trades.length
      );

      // candle processing
      model = new CandleManager;

      // loop day after finished processing trades
      model.on('processed', next);

      model.on('history state', function() {
        // process the trades in this day
        model.processTrades(fetch);
      });

      // prepare the model for processing
      model.setDay(fetch.start);

      // load current history for the day
      model.checkHistory();

    }, this),
    _.bind(function() {
      console.log();
      // restart market manager
      this.historicalStart = false;
      GLOBAL.emitters.market.watching = false;
      GLOBAL.emitters.market.start();
    }, this)
  );

}


module.exports = Fetcher;

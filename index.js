'use strict';

var CacheProvider = require('butter-cache-provider')
var DataStore = require('nedb')

var inherits = require('util').inherits

var DB = {}

function getDB(file) {
    DB[file] = DB[file] || new DataStore({filename: file,  autoload: true})
    return DB[file]
}

var NeDBCacheProvider = function () {
    NeDBCacheProvider.super_.apply(this, arguments)

    this.fetchDB  = getDB(this.configDir + '/nedb-fetch.db')
    this.detailDB = getDB(this.configDir + '/nedb-detail.db')
}

inherits(NeDBCacheProvider, CacheProvider)

NeDBCacheProvider.prototype.fetchFromDB = function (filters) {
    var db = this.fetchDB

    return new Promise ((resolve, reject) => {
        var filters = Object.assign({page: 1}, filters)
        var params = {
            sort: 'rating',
            order: 'asc',
            limit: 50
        }
        var findOpts = {}

        if (filters.keywords) {
            findOpts = {
                title: new RegExp(filters.keywords.replace(/\s/g, '\\s+'), 'gi')
            }
        }

        if (filters.genre) {
            params.genre = filters.genre
        }

        if (filters.order) {
            params.order = filters.order
        }

        if (filters.sorter && filters.sorter !== 'popularity') {
            params.sort = filters.sorter
        }

        var sortOpts = {}
        sortOpts[params.sort] = params.order

        return db.find(findOpts)
                 .sort(sortOpts)
                 .skip((filters.page - 1) * params.limit)
                 .limit(Number(params.limit))
                 .exec((err, docs) =>  {
                     if (err) return reject(err)
                     if (! docs.length) return reject(0)

                     resolve ({
                         results: docs,
                         hasMore: true
                     })
                 })
    })
}

NeDBCacheProvider.prototype.updateFetch = function (data) {
    var db = this.fetchDB
    var uniqueId = this.config.uniqueId

    console.log('Updating fetch data')
    var promises = data.results
                       .filter(r => r)
                       .map(r => Object.assign(r, {_id: r[uniqueId]}))
                       .map(r => (new Promise((accept, reject) =>
                           db.update({_id: r._id}, r, {upsert: true},
                                     (err, num) => (err ? reject(err) : accept(num))))))
    return Promise.all(promises)
                  .catch(console.error.bind(console, 'Error Updating fetch data:'))
}

NeDBCacheProvider.prototype.detailFromDB = function (id, old) {
    var db = this.detailDB

    return Promise.reject(old)
    return new Promise((accept, reject) => (
        db.findOne({_id: id}, (err, doc) => {
            if (err) return reject(err)
            if (!doc) return reject(doc)
            return accept(doc)
        })
    ))
}

NeDBCacheProvider.prototype.updateDetail = function (data) {
    var db = this.detailDB
    var uniqueId = this.config.uniqueId
    var id = data[uniqueId]

    console.log('Updating detail data', id)
    return new Promise((accept, reject) => (
        db.findOne({_id: id}, (err, doc) => {
            if (err) return reject(err)
            if (!doc) return db.insert(data, (err, doc) => err?reject(err):accept(doc))
            if (JSON.stringify(doc) === JSON.stringify(data)) return accept(doc)
            db.update({_id: id}, data, {upsert: true}, (err, num, b, c) => {
                if (err) return reject(err)
                return accept(num)
            })
        })
    )).catch(console.error.bind(console, 'Error Updating detail data:'))
}

module.exports = NeDBCacheProvider


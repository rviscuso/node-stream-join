/**
 * Created by rviscuso on 7/12/17.
 */

const EventEmitter = require('events').EventEmitter;
const PassThrough = require('stream').PassThrough;

module.exports = {

    join(streams, joins){
        streams = handleStreamsArg(streams);
        joins = handleJoins(joins, streams);
        let concatdStream = module.exports.concat(streams.map(s => s.stream));
        return getGroups(concatdStream, streams, joins.map(j => j.join));
    },

    concat: function (streams) {
        let t = new PassThrough({objectMode: true});
        let counter = streams.length;

        streams.forEach((s, i) => {
            s.on('data', x => setImmediate(() => t.push([i, x])));
            s.on('error', err => setImmediate(() => t.emit('error', err)));
            s.on('end', () => {
                if (counter-- <= 1) setImmediate(() => t.push(null))
            })
        });

        return t;
    },


    array2Stream: function(array){
        const retS = new PassThrough({objectMode: true});
        array.forEach(e => setImmediate(() => retS.push(e)));
        setImmediate(() => retS.push(null));
        return retS;
    },
};

class GroupEmitter extends EventEmitter {

    constructor(emitConditionOnPut) {
        super();
        this.groups = {};
        this.emitConditionOnPut = emitConditionOnPut ? emitConditionOnPut : group => false;
    }

    put(group, index, value) {
        if (!this.groups[group]) this.groups[group] = [];
        let currVal = this.groups[group][index];
        if(currVal){
            if(!Array.isArray(currVal)){
                this.groups[group][index] = [currVal];
            }
            this.groups[group][index].push(value);
        }
        else {
            this.groups[group][index] = value
        }
        if (this.emitConditionOnPut(this.groups[group])) this.emit('data', this.groups[group])
    }

    emitAll() {
        Object.keys(this.groups).forEach(g => {
            this.emit('data', this.groups[g]);
        })
    }
}

function getGroups (concatdStream, streams, joins, emitConditionOnPut) {
    let groups = new GroupEmitter(emitConditionOnPut);
    let t = new PassThrough({objectMode: true});

    concatdStream.on('data', x => {
        let idx = x[0];
        let val = x[1];
        let groupKey = [];
        let putGroup = true;
        for (let i = 0; i < joins.length; i++) {
            groupKey.push(val[joins[i][idx]]);
            if (!val[joins[i][idx]]) putGroup = false;
        }
        if (putGroup) {
            groups.put(groupKey, idx, val)
        }
    });

    groups.on('data', group => {
        if(shouldEmit(group, streams)){
            group.forEach((e, i) => {
                if(Array.isArray(e) && !streams[i].squash){
                    e.forEach((f, j) => {
                        let splitGroup = [...group];
                        delete splitGroup[i];
                        splitGroup[i] = e[j];
                        t.push(formatGroupForEmit(splitGroup, streams, joins));
                    })
                }
                else {
                    t.push(formatGroupForEmit(group, streams, joins));
                }
            })
        }
    });

    concatdStream.on('error', err => t.emit(err));

    concatdStream.on('end', () => {
        if (!emitConditionOnPut) groups.emitAll();
        setImmediate(() => groups.removeAllListeners());
        setImmediate(() => t.push(null));
    });

    return t;
}

function shouldEmit(group, streams) {

    let mkeys = missingKeys(group, streams.length);
    return mkeys.length === 0 || mkeys.every(k => streams[k].nullable);

    function missingKeys(group, n){
        let res = [];
        for(let i=0; i < n; i++){
            if(!group[i]) res.push(i);
        }
        return res;
    }
}

function formatGroupForEmit(group, streams, joins) {
    let res = {};
    group.forEach((e, i) => {
        if(!streams[i].includeJoinField) joins.forEach(j => delete e[j[i]]);
        if(Array.isArray(e)) {
            if(streams[i].squash) e = squash(e);
        }
        res = Object.assign(res, streams[i].alias? {[streams[i].alias]: e} : e)
    });
    return res;
}

function squash(array){
    let res = {};
    array.forEach(e => {
        Object.keys(e).forEach(k => {
            if(res[k]){
                if(!Array.isArray(res[k]) && res[k] !== e[k]) res[k] = [res[k]];
                if(res[k].indexOf(e[k]) < 0)res[k].push(e[k]);
            }
            else {
                res[k] = e[k];
            }
        })
    });
    return res;
}

function handleStreamsArg(streams) {
    const ERR_MSG = 'streams must be an array of streams or stream definition objects.';
    if (!Array.isArray(streams)) throw new Error(ERR_MSG);
    if (streams.every(s => isStream(s))) {
        streams = streams.map(s => {return {
            stream: s
        }});
        // Include the join field only for the left
        streams[0].includeJoinField = true;
    }
    else if (!streams.every(s => isStreamDef(s))) {
        throw new Error(ERR_MSG);
    }
    return streams;
}

function handleJoins(joins, streams) {
    const n = streams.length;
    if (!Array.isArray(joins)) throw new Error('joins must be an array of arrays.');
    if (!(joins.every(j => j.length === n && (isJoin(j) || isJoinDef(j))))) {
        throw new Error('Each join must contain a number of string elements equal to the number of streams: ' + n);
    }
    if(joins.every(j => isJoin(j))){
        joins = joins.map(j => {return {
            join: j
        }})
    }
    else if (!joins.every(s => isJoinDef(s))) {
        throw new Error('joins must be an array of arrays or join objects.');
    }
    return joins;
}

function isJoin(o){
    return Array.isArray(o) && o.every(j => typeof j === 'string')
}

function isJoinDef(o){
    return typeof(o) === 'object' && o.join && o.join.every(j => isJoinSet(j));
}

function isStream(o) {
    return o.pipe && o.on && typeof o.pipe === 'function' && typeof o.on === 'function';
}

function isStreamDef(o) {
    return o.stream && isStream(o.stream) &&
        (!o.alias || typeof o.alias === 'string') &&
        (!o.nullable || typeof o.nullable === 'boolean') &&
        (!o.squash || typeof o.squash === 'boolean') &&
        (!o.includeJoinField || typeof o.includeJoinField === 'boolean');
}
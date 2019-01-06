
const EventEmitter = require('events').EventEmitter;
const PassThrough = require('stream').PassThrough;
const { squash, flatten, cartesian } = require('./array-utils');

function concat(streams, addIndex) {
    let t = new PassThrough({ objectMode: true });
    let counter = streams.length;

    streams.forEach((s, i) => {
        s.on('data', x => setImmediate(() => {
            if (addIndex) t.push([i, x]);
            else t.push(x);
        }));
        s.on('error', err => setImmediate(() => t.emit('error', err)));
        s.on('end', () => {
            if (counter-- <= 1) setImmediate(() => t.push(null))
        })
    });

    return t;
}

module.exports = (streams, joins) => {
    streams = handleStreamsArg(streams);
    if (!joins) {
        return concat(streams.map(s => s.stream), false);
    }
    let concatdStream = concat(streams.map(s => s.stream), true);
    joins = handleJoins(joins, streams);
    return getGroups(concatdStream, streams, joins.map(j => j.join));
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
        if (currVal) {
            if (!Array.isArray(currVal)) {
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

function getGroups(concatdStream, streams, joins, emitConditionOnPut) {
    let groups = new GroupEmitter(emitConditionOnPut);
    let t = new PassThrough({ objectMode: true });

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
        if (shouldEmit(group, streams)) {
            group = group.map((g, i) => {
                if (Array.isArray(g)) {
                    if (streams[i].squash) return squash(g);
                }
                return g;
            });
            let splitGroups = cartesian(group);
            splitGroups.forEach(g => t.push(formatGroupForEmit(g, streams, joins)))
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

    function missingKeys(group, n) {
        let res = [];
        for (let i = 0; i < n; i++) {
            if (!group[i]) res.push(i);
        }
        return res;
    }
}

function formatGroupForEmit(group, streams, joins) {
    return group.reduce(
        (m, c, i) => {
            let d = Object.assign({}, c);
            if (streams[i].omitJoinField) joins.forEach(j => delete d[j[i]]);
            return Object.assign(m, streams[i].alias ? { [streams[i].alias]: d } : d)
        },
        {}
    )
}

function handleStreamsArg(streams) {
    const ERR_MSG = 'streams must be an array of streams or stream definition objects.';
    if (!Array.isArray(streams)) throw new Error(ERR_MSG);
    if (streams.every(s => isStream(s))) {
        streams = streams.map(s => {
            return {
                stream: s
            }
        });
        // Include the join field only for the left
        for (let i = 1; i < streams.length; i++) {
            streams[i].omitJoinField = true;
        }
    }
    else if (!streams.every(s => isStreamDef(s))) {
        throw new Error(ERR_MSG);
    }
    return streams;
}

function handleJoins(joins, streams) {
    const n = streams.length;
    if (!Array.isArray(joins)) {
        joins = [joins]
    }
    joins = joins.map(j => {
        if (typeof j === 'string' || j instanceof String) {
            return new Array(n).fill(j);
        }
        return j;
    })
    if (!(joins.every(j => j.length === n && isJoin(j)))) {
        throw new Error('Each join must contain a number of string elements equal to the number of streams: ' + n);
    }
    if (joins.every(j => isJoin(j))) {
        joins = joins.map(j => {
            return {
                join: j
            }
        })
    }
    else if (!joins.every(s => isJoinDef(s))) {
        throw new Error('joins must be an array of arrays or join objects.');
    }
    return joins;
}

function isJoin(o) {
    return Array.isArray(o) && o.every(j => typeof j === 'string')
}

function isStream(o) {
    return o && o.pipe && o.on && typeof o.pipe === 'function' && typeof o.on === 'function';
}

function isStreamDef(o) {
    return o.stream && isStream(o.stream) &&
        (!o.alias || typeof o.alias === 'string') &&
        (!o.nullable || typeof o.nullable === 'boolean') &&
        (!o.squash || typeof o.squash === 'boolean') &&
        (!o.omitJoinField || typeof o.omitJoinField === 'boolean');
}
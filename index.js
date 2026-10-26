
const PassThrough = require('stream').PassThrough;
const { squash, cartesian } = require('./array-utils');

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

function getGroups(concatdStream, streams, joins) {
    const n = streams.length;
    let t = new PassThrough({ objectMode: true });
    // group key -> one array of rows per stream. Rows are kept until the end, because
    // a later row can still match them. No prototype: keys come from data, and a key
    // like '__proto__' must not resolve to Object.prototype (prototype pollution).
    const groups = Object.create(null);
    // squash merges all of a stream's matches into one row, so it needs every row
    // first: joins with a squash stream emit everything at the end.
    const deferAll = streams.some(s => s.squash);

    const isComplete = group => group.every(rows => rows.length > 0);
    const push = slots => cartesian(slots).forEach(g => t.push(formatGroupForEmit(g, streams, joins)));

    // Final emit of a group: unmatched (outer) rows, squashed rows, or deferred groups
    const emitFinal = group => {
        if (!shouldEmit(group, streams)) return;
        push(group.map((rows, i) => {
            // undefined keeps the slot, so each row stays aligned with its own stream
            if (rows.length === 0) return [undefined];
            if (streams[i].squash) return [squash(rows)];
            return rows;
        }));
    };

    concatdStream.on('data', x => {
        let idx = x[0];
        let val = x[1];
        let groupKey = [];
        let putGroup = true;
        for (let i = 0; i < joins.length; i++) {
            const value = getField(val, joins[i][idx]);
            groupKey.push(value);
            // Only null/undefined mean "no join value"; 0 and '' are real keys.
            if (value == null) putGroup = false;
        }
        if (!putGroup) {
            // A null join key never matches, so the row is its own unmatched group and
            // can be emitted now. shouldEmit() keeps it only if every other stream is
            // nullable (outer join).
            const group = Array.from({ length: n }, () => []);
            group[idx].push(val);
            emitFinal(group);
            return;
        }
        // Composite keys are JSON-encoded so ['a,b', 'c'] and ['a', 'b,c'] stay apart
        // (String(array) joins with commas). Single-field keys keep their old string
        // form. String() keeps 1 and '1' matching, as before.
        const key = groupKey.length === 1 ? String(groupKey[0]) : JSON.stringify(groupKey.map(String));
        if (!groups[key]) groups[key] = Array.from({ length: n }, () => []);
        const group = groups[key];
        group[idx].push(val);
        if (!deferAll && isComplete(group)) {
            // Emit as soon as every stream has a match, but only the combinations that
            // include the new row: the others were emitted when their rows arrived.
            push(group.map((rows, i) => i === idx ? [val] : rows));
        }
    });

    concatdStream.on('error', err => t.emit('error', err));

    concatdStream.on('end', () => {
        Object.keys(groups).forEach(key => {
            const group = groups[key];
            // Complete groups were already emitted as rows arrived, unless deferred
            if (deferAll || !isComplete(group)) emitFinal(group);
        });
        setImmediate(() => t.push(null));
    });

    return t;
}

// Emit a group when every stream has a row, or every stream without one is nullable
function shouldEmit(group, streams) {
    return group.every((rows, i) => rows.length > 0 || streams[i].nullable);
}

function formatGroupForEmit(group, streams, joins) {
    return group.reduce(
        (m, c, i) => {
            if (c === undefined) return m; // no row from this (nullable) stream
            let d = Object.assign({}, c);
            if (streams[i].omitJoinField) joins.forEach(j => { d = omitField(d, j[i]); });
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

const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

// A join field is a key, or a path like 'user.id' or 'items[0].sku' (#3). A key that
// exists exactly as written wins, so fields whose names contain dots keep working.
function fieldPath(row, field) {
    if (row != null && hasOwn(row, field)) return [field];
    return field.replace(/\[(\d+)\]/g, '.$1').split('.');
}

// Own properties only: an inherited one like 'constructor' would give every row
// the same join value, so rows without the field would all match each other.
function getField(row, field) {
    let value = row;
    for (const key of fieldPath(row, field)) {
        if (value == null || typeof value !== 'object' || !hasOwn(value, key)) return undefined;
        value = value[key];
    }
    return value;
}

// Copy of row without the field. Objects along a path are copied, never changed,
// because they belong to the caller's input rows.
function omitField(row, field) {
    return omitPath(row, fieldPath(row, field));
}

function omitPath(value, keys) {
    if (value == null || typeof value !== 'object' || !hasOwn(value, keys[0])) return value;
    const copy = Array.isArray(value) ? value.slice() : Object.assign({}, value);
    if (keys.length === 1) delete copy[keys[0]];
    else copy[keys[0]] = omitPath(value[keys[0]], keys.slice(1));
    return copy;
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
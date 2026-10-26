# node-stream-join

This package provides the ability to perform SQL-like joins on node.js streams.

[![test](https://github.com/rviscuso/node-stream-join/actions/workflows/test.yml/badge.svg)](https://github.com/rviscuso/node-stream-join/actions/workflows/test.yml)

## Usage

```javascript
const join = require('node-stream-join');

const joinedStream = join(<streams> , <joinConditions>)
```

### When rows are emitted

Joined rows are emitted as soon as they are known, without waiting for the input streams to end:

- A row is emitted as soon as its join key has a match in every stream. If more matching rows arrive later, only the new combinations are emitted.
- Unmatched rows of an outer join (see `nullable`) are emitted when the input streams end, because only then is it certain that no match will arrive. A row whose join field is null or undefined can never match, so it is emitted right away.
- If any stream uses `squash`, the whole join is emitted when the input streams end, because squashing needs every match.

Output order follows when each match completes, not the order of the input. Every row seen so far is kept in memory until the input streams end, since a later row can still match it.

## Examples

### Simple Join On Same Key

```javascript
const firstNames = new PassThrough({objectMode: true});
const lastNames = new PassThrough({objectMode: true});

join([firstNames, lastNames], 'id')
    .on('data', console.log);

firstNames.push({firstName: 'Cosmo', id: 1});
firstNames.push({firstName: 'Elaine', id: 2});
firstNames.push({firstName: 'George', id: 3});
firstNames.push({firstName: 'Jerry', id: 4});

lastNames.push({lastName: 'Benes', id: 2});
lastNames.push({lastName: 'Seinfeld', id: 4});
lastNames.push({lastName: 'Kramer', id: 1});
lastNames.push({lastName: 'Costanza', id: 3});

firstNames.push(null);
lastNames.push(null);
// { firstName: 'Cosmo', id: 1, lastName: 'Kramer' }
// { firstName: 'Elaine', id: 2, lastName: 'Benes' }
// { firstName: 'George', id: 3, lastName: 'Costanza' }
// { firstName: 'Jerry', id: 4, lastName: 'Seinfeld' }
```

### Simple Join On Different Keys

```javascript
const firstNames = new PassThrough({objectMode: true});
const lastNames = new PassThrough({objectMode: true});

join([firstNames, lastNames], [['id', 'pid']]) // equivalent to id===pid
    .on('data', console.log);

firstNames.push({firstName: 'Cosmo', id: 1});
firstNames.push({firstName: 'Elaine', id: 2});
firstNames.push({firstName: 'George', id: 3});
firstNames.push({firstName: 'Jerry', id: 4});

lastNames.push({lastName: 'Benes', pid: 2});
lastNames.push({lastName: 'Seinfeld', pid: 4});
lastNames.push({lastName: 'Kramer', pid: 1});
lastNames.push({lastName: 'Costanza', pid: 3});

firstNames.push(null);
lastNames.push(null);
// { firstName: 'Cosmo', id: 1, lastName: 'Kramer' }
// { firstName: 'Elaine', id: 2, lastName: 'Benes' }
// { firstName: 'George', id: 3, lastName: 'Costanza' }
// { firstName: 'Jerry', id: 4, lastName: 'Seinfeld' }
```

### Composite Join On Different Fields

```javascript
const firstNames = new PassThrough({objectMode: true});
const lastNames = new PassThrough({objectMode: true});

join(
    [firstNames, lastNames], 
    [['firstName', 'fname'], ['lastName', 'lname']] // equivalent to firstName===fname && lastName===lname
    ) 
    .on('data', console.log);

firstNames.push({firstName: 'Cosmo', lastName: 'Kramer', id: 1});
firstNames.push({firstName: 'Elaine', lastName: 'Benes', id: 2});
firstNames.push({firstName: 'George', lastName: 'Costanza', id: 3});
firstNames.push({firstName: 'Jerry', lastName: 'Seinfeld', id: 4});

lastNames.push({fname: 'Elaine', lname: 'Benes', pid: 2});
lastNames.push({fname: 'Jerry', lname: 'Seinfeld', pid: 4});
lastNames.push({fname: 'Cosmo', lname: 'Kramer', pid: 1});
lastNames.push({fname: 'George', lname: 'Costanza', pid: 3});

firstNames.push(null);
lastNames.push(null);
// { firstName: 'Cosmo', id: 1, pid: 1, lastName: 'Kramer' }
// { firstName: 'Elaine', id: 2, pid: 2, lastName: 'Benes' }
// { firstName: 'George', id: 3, pid: 3, lastName: 'Costanza' }
// { firstName: 'Jerry', id: 4, pid: 4, lastName: 'Seinfeld' }
```

### Concatenation Only, no Join

```javascript
const numbers = new PassThrough({objectMode: true});
const letters = new PassThrough({objectMode: true});

join([numbers, letters])
    .on('data', console.log);

numbers.push({value: 1});
letters.push({value: 'a'});
numbers.push({value: 2});
numbers.push(null);
letters.push({value: 'b'});
letters.push(null);

// {value: 1}
// {value: 'a'}
// {value: 2}
// {value: 'b'}
```

## Stream Options
To use all defaults, simply provide the stream as the first argument. To use non-defaults, provide instead an object with a stream field (mandatory) and any of the options below:

### alias (default: none)
When set for a given stream, the join output will add a container object around the fields from that stream

### squash (default: false)
By default, if a given stream has more than one join match on the other streams, each match will be emitted separately. With this option, all matches in a given stream will be emitted only once, with fields with different values contained inside an array.

### nullable (default: false)
By default, node-stream-join performs an inner join: a row is emitted only when its join key has a match in every stream, so rows without a match, including rows whose join field is null or undefined, are dropped.

Setting nullable on a stream means that stream may be missing from a joined row, which turns the join into an outer join. For example, making the second of two streams nullable gives a left join: every row of the first stream is emitted, with the matching fields of the second stream when there is a match, including rows of the first stream whose join field is null. Join values of 0 and empty string are regular keys.

### omitJoinField (default: false)
By default, the join fields will be part of each emitted object. You can overwrite this behavior for each stream by setting omitJoinField to true.

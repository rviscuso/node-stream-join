# node-stream-join

This package provides the ability to perform SQL-like joins in node streams.

[![Build Status](https://travis-ci.org/rviscuso/node-stream-join.svg?branch=master)](https://travis-ci.org/rviscuso/node-stream-join)

## Usage

```javascript
const join = require('node-stream-join');

const joinedStream = join(<streams> , <joinConditions>)
```

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
// { firstName: 'Cosmo', id: 1, pid: 1, lastName: 'Kramer' }
// { firstName: 'Elaine', id: 2, pid: 2, lastName: 'Benes' }
// { firstName: 'George', id: 3, pid: 3, lastName: 'Costanza' }
// { firstName: 'Jerry', id: 4, pid: 4, lastName: 'Seinfeld' }
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
By default, if a given stream has more than one join match on the other streams, each match will be emitted separately. With this options, all matches in a given stream will be emitted only once, with fields with different values contained inside an array.

### nullable (default: false)
By default, node-stream-join performs an inner join, i.e. objects with null join fields will not be emitted. The nullable option turns it into an outer join, i.e. objects with the join field null will be emitted.

### omitJoinField (default: false)
By default, the join fields will be part of each emitted object. You can overwrite this behavior for each stream by setting omitJoinField to true.

/**
 * Created by rviscuso on 7/12/17.
 */

require('chai').should();
const _ = require('highland');
const PassThrough = require('stream').PassThrough;

const join = require('../index');

const a1 = [
    { name: 'Cosmo', ss: 12345 },
    { name: 'Kramer', ss: 12345 },
    { name: 'Kramer', ss: 11111 },
    { name: 'Elaine', ss: 23456 },
    { name: 'George', ss: 34567 },
    { name: 'Newman', ss: 45678 },
    { name: 'Jerry', ss: 56789 },
];

const a2 = [
    { ssn: 12345, name: 'Kramer', score: 500 },
    { ssn: 23456, name: 'Elaine', score: 600 },
    { ssn: 34567, name: 'George', score: 700 },
    { ssn: 45678, name: 'Newman', score: 800 },
    { ssn: 56789, name: 'Jerry', score: 750 },
    { ssn: 99999, name: 'Frank', score: 750 },
];

const a3 = [
    { social: 12345, dl: 11111 },
    { social: 23456, dl: 22222 },
    { social: 34567, dl: 33333 },
    { social: 45678, dl: 44444 },
    { social: 56789, dl: 55555 },
];

let s1, s2, s3;

describe('readme', function () {

    it('concatenates two streams', function () {
        const s1 = new PassThrough({ objectMode: true });
        const s2 = new PassThrough({ objectMode: true });

        const joined = join([s1, s2]);

        s1.push({ value: 1 });
        s2.push({ value: 'a' });
        s1.push({ value: 2 });
        s1.push(null);
        s2.push({ value: 'b' });
        s2.push(null);

        return _(joined)
            .collect()
            .toPromise(Promise)
            .then(x => x.should.eql([
                { value: 1 },
                { value: 'a' },
                { value: 2 },
                { value: 'b' },
            ]));
    })

    it('joins on single field', function () {
        const firstNames = new PassThrough({ objectMode: true });
        const lastNames = new PassThrough({ objectMode: true });

        const joined = join([firstNames, lastNames], 'id');

        firstNames.push({ firstName: 'Cosmo', id: 1 });
        firstNames.push({ firstName: 'Elaine', id: 2 });
        firstNames.push({ firstName: 'George', id: 3 });
        firstNames.push({ firstName: 'Jerry', id: 4 });

        lastNames.push({ lastName: 'Benes', id: 2 });
        lastNames.push({ lastName: 'Seinfeld', id: 4 });
        lastNames.push({ lastName: 'Kramer', id: 1 });
        lastNames.push({ lastName: 'Costanza', id: 3 });

        firstNames.push(null);
        lastNames.push(null);

        return _(joined)
            .collect()
            .toPromise(Promise)
            .then(x => x.should.eql([
                { firstName: 'Cosmo', id: 1, lastName: 'Kramer' },
                { firstName: 'Elaine', id: 2, lastName: 'Benes' },
                { firstName: 'George', id: 3, lastName: 'Costanza' },
                { firstName: 'Jerry', id: 4, lastName: 'Seinfeld' },
            ]))
    })

    it('joins on different fields', function () {
        const firstNames = new PassThrough({ objectMode: true });
        const lastNames = new PassThrough({ objectMode: true });

        const joined = join([firstNames, lastNames], [['id', 'pid']]);

        firstNames.push({ firstName: 'Cosmo', id: 1 });
        firstNames.push({ firstName: 'Elaine', id: 2 });
        firstNames.push({ firstName: 'George', id: 3 });
        firstNames.push({ firstName: 'Jerry', id: 4 });

        lastNames.push({ lastName: 'Benes', pid: 2 });
        lastNames.push({ lastName: 'Seinfeld', pid: 4 });
        lastNames.push({ lastName: 'Kramer', pid: 1 });
        lastNames.push({ lastName: 'Costanza', pid: 3 });

        firstNames.push(null);
        lastNames.push(null);

        return _(joined)
            .collect()
            .toPromise(Promise)
            .then(x => x.should.eql([
                { firstName: 'Cosmo', id: 1, lastName: 'Kramer' },
                { firstName: 'Elaine', id: 2, lastName: 'Benes' },
                { firstName: 'George', id: 3, lastName: 'Costanza' },
                { firstName: 'Jerry', id: 4, lastName: 'Seinfeld' },
            ]))
    })

    it('joins on composite fields', function () {
        const firstNames = new PassThrough({ objectMode: true });
        const lastNames = new PassThrough({ objectMode: true });

        const joined = join(
            [firstNames, lastNames],
            [['firstName', 'fname'], ['lastName', 'lname']]
        )

        firstNames.push({ firstName: 'Cosmo', lastName: 'Kramer', id: 1 });
        firstNames.push({ firstName: 'Elaine', lastName: 'Benes', id: 2 });
        firstNames.push({ firstName: 'George', lastName: 'Costanza', id: 3 });
        firstNames.push({ firstName: 'Jerry', lastName: 'Seinfeld', id: 4 });

        lastNames.push({ fname: 'Elaine', lname: 'Benes', pid: 2 });
        lastNames.push({ fname: 'Jerry', lname: 'Seinfeld', pid: 4 });
        lastNames.push({ fname: 'Cosmo', lname: 'Kramer', pid: 1 });
        lastNames.push({ fname: 'George', lname: 'Costanza', pid: 3 });

        firstNames.push(null);
        lastNames.push(null);

        return _(joined)
            .collect()
            .toPromise(Promise)
            .then(x => x.should.eql([
                { firstName: 'Cosmo', id: 1, lastName: 'Kramer', pid: 1 },
                { firstName: 'Elaine', id: 2, lastName: 'Benes', pid: 2 },
                { firstName: 'George', id: 3, lastName: 'Costanza', pid: 3 },
                { firstName: 'Jerry', id: 4, lastName: 'Seinfeld', pid: 4 },
            ]))
    })
})

describe('join', function () {

    beforeEach(function () {
        s1 = _(a1);
        s2 = _(a2);
        s3 = _(a3);
    });
    it('join on single field name, no squash', function () {
        return _(join([s1, s2], 'name'))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on single field name in array, no squash', function () {
        return _(join([s1, s2], ['name']))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on same field name, no squash', function () {
        return _(join([s1, s2], [['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on same field name, yes squash', function () {
        return _(join([{ stream: s1, squash: true }, { stream: s2 }], [['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(5);
                r.map(x => x.name).should.eql(['Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on different field names', async function () {
        return _(join([s1, s2, s3], [['ss', 'ssn', 'social']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join on 2 different fields', async function () {
        return _(join([s1, s2], [['ss', 'ssn'], ['name', 'name']]))
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(5);
                r.map(x => x.name).should.eql(['Kramer', 'Elaine', 'George', 'Newman', 'Jerry']);
            });
    });

    it('join streams with nullable true', async function () {
        return _(join(
            [{ stream: s1, nullable: true, includeJoinField: true }, { stream: s2, nullable: true }],
            [['ss', 'ssn']])
        )
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(8);
                r.map(x => x.name).should.eql(['Kramer', 'Kramer', 'Kramer', 'Elaine', 'George', 'Newman', 'Jerry', 'Frank']);
                // Note Cosmo gets overwritten by Kramer since they have the same SSN, so this is correct
            });
    });

    it('join streams with aliases', async function () {
        return _(join(
            [{ stream: s1, alias: 'ssnData' }, { stream: s2, alias: 'creditData' }],
            [['ss', 'ssn']])
        )
            .collect()
            .toPromise(Promise)
            .then(r => {
                r.length.should.equal(6);
                r.forEach(x => x.should.include.keys(['ssnData', 'creditData']));
            });
    })

});